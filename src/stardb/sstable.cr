require "./value"
require "./bloom_filter"

module StarDB
  # Sorted String Table
  # Immutable on-disk data structure
  class SSTable
    MAGIC = "STDB"
    VERSION = 1_u32

    struct IndexEntry
      getter key : String
      getter offset : UInt64
      getter size : UInt32

      def initialize(@key : String, @offset : UInt64, @size : UInt32)
      end

      def serialize(io : IO)
        io.write_bytes(@key.bytesize.to_u32, IO::ByteFormat::LittleEndian)
        io.write(@key.to_slice)
        io.write_bytes(@offset, IO::ByteFormat::LittleEndian)
        io.write_bytes(@size, IO::ByteFormat::LittleEndian)
      end

      def self.deserialize(io : IO) : IndexEntry
        key_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
        key_bytes = Bytes.new(key_size)
        io.read_fully(key_bytes)
        key = String.new(key_bytes)
        offset = io.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
        size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
        new(key, offset, size)
      end
    end

    struct DataEntry
      getter key : String
      getter value : Value?
      getter deleted : Bool
      getter timestamp : Int64

      def initialize(@key : String, @value : Value?, @deleted : Bool, @timestamp : Int64)
      end

      def serialize(io : IO)
        io.write_bytes(@timestamp, IO::ByteFormat::LittleEndian)
        io.write_byte(@deleted ? 1_u8 : 0_u8)
        
        io.write_bytes(@key.bytesize.to_u32, IO::ByteFormat::LittleEndian)
        io.write(@key.to_slice)
        
        if val = @value
          io.write_byte(1_u8)
          val.serialize(io)
        else
          io.write_byte(0_u8)
        end
      end

      def self.deserialize(io : IO) : DataEntry
        timestamp = io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
        deleted = io.read_byte.not_nil! == 1_u8
        
        key_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
        key_bytes = Bytes.new(key_size)
        io.read_fully(key_bytes)
        key = String.new(key_bytes)
        
        has_value = io.read_byte.not_nil! == 1_u8
        value = has_value ? Value.deserialize(io) : nil
        
        new(key, value, deleted, timestamp)
      end
    end

    @path : String
    @index : Array(IndexEntry)
    @bloom : BloomFilter
    @file : File?
    @min_key : String
    @max_key : String

    def initialize(@path : String)
      @index = [] of IndexEntry
      @bloom = BloomFilter.new(1024)
      @min_key = ""
      @max_key = ""
      load_metadata
    end

    def self.create(path : String, entries : Array(Tuple(String, Value?, Bool, Int64)))
      file = File.open(path, "w")
      file.write(MAGIC.to_slice)
      file.write_bytes(VERSION, IO::ByteFormat::LittleEndian)
      
      sorted = entries.sort_by { |e| e[0] }
      
      bloom = BloomFilter.new(BloomFilter.optimal_size(sorted.size), 
                              BloomFilter.optimal_hash_count(BloomFilter.optimal_size(sorted.size), sorted.size))
      sorted.each { |e| bloom.add(e[0]) }
      
      metadata_offset_pos = file.pos
      file.write_bytes(0_u64, IO::ByteFormat::LittleEndian)
      
      index = [] of IndexEntry
      sorted.each do |key, value, deleted, timestamp|
        offset = file.pos.to_u64
        start_pos = file.pos     
        entry = DataEntry.new(key, value, deleted, timestamp)
        entry.serialize(file)
        size = (file.pos - start_pos).to_u32
        index << IndexEntry.new(key, offset, size)
      end
      
      metadata_offset = file.pos.to_u64
      file.write_bytes(index.size.to_u32, IO::ByteFormat::LittleEndian)
      index.each { |entry| entry.serialize(file) }
      bloom.serialize(file)
      min_key = sorted.first[0]
      max_key = sorted.last[0]

      file.write_bytes(min_key.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      file.write(min_key.to_slice)
      file.write_bytes(max_key.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      file.write(max_key.to_slice)
      file.seek(metadata_offset_pos)
      file.write_bytes(metadata_offset, IO::ByteFormat::LittleEndian)      
      file.close
      
      new(path)
    end

    def get(key : String) : Value?
      return nil unless @bloom.might_contain?(key)
      return nil if key < @min_key || key > @max_key
      
      idx = binary_search(key)
      return nil if idx.nil?
      
      entry_meta = @index[idx]
      file = get_file
      file.seek(entry_meta.offset)
      
      entry = DataEntry.deserialize(file)
      entry.deleted ? nil : entry.value
    end

    def scan(start_key : String? = nil, end_key : String? = nil, &block : String, Value?, Bool, Int64 ->)
      file = get_file
      
      @index.each do |entry_meta|
        next if start_key && entry_meta.key < start_key
        break if end_key && entry_meta.key > end_key
        
        file.seek(entry_meta.offset)
        entry = DataEntry.deserialize(file)
        yield entry.key, entry.value, entry.deleted, entry.timestamp
      end
    end

    def min_key : String
      @min_key
    end

    def max_key : String
      @max_key
    end

    def size : Int32
      @index.size
    end

    def close
      @file.try(&.close)
      @file = nil
    end

    private def load_metadata
      file = File.open(@path, "r")
      
      magic = Bytes.new(4)
      file.read_fully(magic)
      raise "Invalid SSTable file" unless String.new(magic) == MAGIC
      
      version = file.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      raise "Unsupported version" unless version == VERSION
      
      metadata_offset = file.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
      file.seek(metadata_offset)
      
      index_size = file.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      @index = Array(IndexEntry).new(index_size)
      index_size.times do
        @index << IndexEntry.deserialize(file)
      end
      
      @bloom = BloomFilter.deserialize(file)
      
      min_key_size = file.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      min_key_bytes = Bytes.new(min_key_size)
      file.read_fully(min_key_bytes)
      @min_key = String.new(min_key_bytes)
      
      max_key_size = file.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      max_key_bytes = Bytes.new(max_key_size)
      file.read_fully(max_key_bytes)
      @max_key = String.new(max_key_bytes)
      
      file.close
    end

    private def binary_search(key : String) : Int32?
      left = 0
      right = @index.size - 1
      
      while left <= right
        mid = (left + right) // 2
        mid_key = @index[mid].key
        
        if mid_key == key
          return mid
        elsif mid_key < key
          left = mid + 1
        else
          right = mid - 1
        end
      end
      
      nil
    end

    private def get_file : File
      @file ||= File.open(@path, "r")
    end
  end
end
