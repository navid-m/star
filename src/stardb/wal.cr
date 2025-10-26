require "./value"

module StarDB
  # Write-ahead log.
  # Generally ensures durability.
  class WAL
    enum EntryType : UInt8
      Put
      Delete
    end

    struct Entry
      getter type : EntryType
      getter key : String
      getter value : Value?
      getter timestamp : Int64

      def initialize(@type : EntryType, @key : String, @value : Value? = nil, @timestamp : Int64 = Time.utc.to_unix_ms)
      end

      def serialize(io : IO)
        io.write_byte(@type.value)
        io.write_bytes(@timestamp, IO::ByteFormat::LittleEndian)
        io.write_bytes(@key.bytesize.to_u32, IO::ByteFormat::LittleEndian)
        io.write(@key.to_slice)
        
        if val = @value
          io.write_byte(1_u8)
          val.serialize(io)
        else
          io.write_byte(0_u8)
        end
      end

      def self.deserialize(io : IO) : Entry
        type_byte = io.read_byte
        
        raise IO::EOFError.new if type_byte.nil?

        type = EntryType.new(type_byte)
        timestamp = io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
        key_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
        key_bytes = Bytes.new(key_size)
        
        io.read_fully(key_bytes)

        key = String.new(key_bytes)
        has_value_byte = io.read_byte

        raise IO::EOFError.new if has_value_byte.nil?
        
        has_value = has_value_byte == 1_u8
        value = has_value ? Value.deserialize(io) : nil
        
        new(type, key, value, timestamp)
      end
    end

    @file : File
    @path : String
    @mutex : Mutex
    @sync_on_write : Bool
    @buffer : IO::Memory
    @buffer_size : Int32

    def initialize(@path : String, @sync_on_write : Bool = false, @buffer_size : Int32 = 64 * 1024)
      @file = File.open(@path, "a+")
      @mutex = Mutex.new
      @buffer = IO::Memory.new(@buffer_size)
    end

    def append(entry : Entry)
      @mutex.synchronize do
        return if @file.closed?
        entry.serialize(@buffer)
        
        if @buffer.pos >= @buffer_size - 1024
          flush_buffer
        end
      end
    end

    def append_batch(entries : Array(Entry))
      @mutex.synchronize do
        return if @file.closed?
        entries.each { |entry| entry.serialize(@buffer) }
        flush_buffer
      end
    end

    private def flush_buffer
      return if @buffer.pos == 0
      size = @buffer.pos
      @buffer.rewind
      @file.write(@buffer.to_slice[0, size])
      @file.flush
      @file.fsync if @sync_on_write
      @buffer.clear
      @buffer.rewind
    end

    def put(key : String, value : Value)
      append(Entry.new(EntryType::Put, key, value))
    end

    def delete(key : String)
      append(Entry.new(EntryType::Delete, key))
    end

    def replay(&block : Entry ->)
      @mutex.synchronize do
        @file.rewind
        while !@file.closed?
          begin
            entry = Entry.deserialize(@file)
            yield entry
          rescue IO::EOFError
            break
          end
        end
      end
    end

    def truncate
      @mutex.synchronize do
        return if @file.closed?
        flush_buffer
        @file.truncate(0)
        @file.rewind
      end
    end

    def close
      @mutex.synchronize do
        flush_buffer unless @file.closed?
        @file.close unless @file.closed?
      end
    end

    def flush
      @mutex.synchronize do
        flush_buffer unless @file.closed?
      end
    end

    def self.recover(path : String, &block : Entry ->)
      return unless File.exists?(path)
      wal = new(path)
      wal.replay { |entry| yield entry }
      wal.close
    end
  end
end
