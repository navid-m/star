require "./memtable"
require "./sstable"
require "./wal"
require "./compaction"
require "./value"

module StarDB
  # Main database interface
  class Database
    MEMTABLE_FLUSH_SIZE = 64 * 1024 * 1024

    @path : String
    @memtable : MemTable
    @immutable_memtables : Array(MemTable)
    @wal : WAL
    @compaction : CompactionManager
    @mutex : Mutex
    @flush_fiber : Fiber?
    @running : Bool
    @sync_on_write : Bool

    def initialize(@path : String, @sync_on_write : Bool = false)
      Dir.mkdir_p(@path)
      
      @memtable = MemTable.new
      @immutable_memtables = [] of MemTable
      @wal = WAL.new(File.join(@path, "wal.log"), @sync_on_write)
      @compaction = CompactionManager.new(@path)
      @mutex = Mutex.new
      @running = true
      
      recover_from_wal
      load_sstables
      
      @compaction.start_background_compaction
      
      start_flush_worker
    end

    def put(key : String, value : Bool)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : Int8)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : Int16)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : Int32)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : Int64)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : UInt8)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : UInt16)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : UInt32)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : UInt64)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : Float32)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : Float64)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : String)
      put_value(key, Value.from(value))
    end

    def put(key : String, value : Bytes)
      put_value(key, Value.from(value))
    end

    private def put_value(key : String, value : Value)
      return unless @running
      @mutex.synchronize do
        @wal.put(key, value)
        @memtable.put(key, value)
        
        if @memtable.byte_size >= MEMTABLE_FLUSH_SIZE
          rotate_memtable
        end
      end
    end

    def get(key : String) : Value?
      if val = @memtable.get(key)
        return val
      end
      
      @mutex.synchronize do
        @immutable_memtables.reverse_each do |imm|
          if val = imm.get(key)
            return val
          end
        end
      end
      
      @compaction.get_sstables.reverse_each do |sstable|
        if val = sstable.get(key)
          return val
        end
      end
      nil
    end

    def delete(key : String)
      return unless @running
      @mutex.synchronize do
        @wal.delete(key)
        @memtable.delete(key)
        
        if @memtable.byte_size >= MEMTABLE_FLUSH_SIZE
          rotate_memtable
        end
      end
    end

    def scan(start_key : String? = nil, end_key : String? = nil, &block : String, Value ->)
      entries = {} of String => Tuple(Value?, Int64)
      
      @compaction.get_sstables.each do |sstable|
        sstable.scan(start_key, end_key) do |key, value, deleted, timestamp|
          existing = entries[key]?
          if existing.nil? || timestamp > existing[1]
            entries[key] = {deleted ? nil : value, timestamp}
          end
        end
      end
      
      @mutex.synchronize do
        @immutable_memtables.each do |imm|
          imm.each do |key, value, deleted, timestamp|
            next if start_key && key < start_key
            break if end_key && key > end_key
            
            existing = entries[key]?
            if existing.nil? || timestamp > existing[1]
              entries[key] = {deleted ? nil : value, timestamp}
            end
          end
        end
      end
      
      @memtable.each do |key, value, deleted, timestamp|
        next if start_key && key < start_key
        break if end_key && key > end_key
        
        existing = entries[key]?
        if existing.nil? || timestamp > existing[1]
          entries[key] = {deleted ? nil : value, timestamp}
        end
      end
      
      entries.keys.sort.each do |key|
        value, _ = entries[key]
        yield key, value if value
      end
    end

    def compact
      @compaction.compact_if_needed(2)
    end

    def close
      return unless @running
      @running = false
      
      sleep 0.2.seconds
      flush_immutable_memtables
      
      @compaction.close
      @wal.close
    end

    private def recover_from_wal
      WAL.recover(File.join(@path, "wal.log")) do |entry|
        case entry.type
        when WAL::EntryType::Put
          @memtable.put(entry.key, entry.value.not_nil!)
        when WAL::EntryType::Delete
          @memtable.delete(entry.key)
        end
      end
    end

    private def load_sstables
      Dir.glob(File.join(@path, "*.sst")).sort.each do |path|
        sstable = SSTable.new(path)
        @compaction.add_sstable(sstable)
      end
    end

    private def rotate_memtable
      @immutable_memtables << @memtable
      @memtable = MemTable.new
      @wal.truncate
    end

    private def start_flush_worker
      @flush_fiber = spawn do
        while @running
          sleep 1.second
          flush_immutable_memtables if @running
        end
      end
    end

    private def flush_immutable_memtables
      tables_to_flush = @mutex.synchronize do
        tables = @immutable_memtables.dup
        @immutable_memtables.clear
        tables
      end
      
      tables_to_flush.each do |table|
        flush_memtable_to_sstable(table)
      end
    end

    private def flush_memtable_to_sstable(memtable : MemTable)
      entries = [] of Tuple(String, Value?, Bool, Int64)
      memtable.each do |key, value, deleted, timestamp|
        entries << {key, value, deleted, timestamp}
      end
      
      return if entries.empty?
      
      timestamp = Time.utc.to_unix_ms
      path = File.join(@path, "sstable_#{timestamp}.sst")
      SSTable.create(path, entries)
      sstable = SSTable.new(path)

      @compaction.add_sstable(sstable)
    end
  end
end
