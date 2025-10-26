require "./sstable"
require "./memtable"

module StarDB
  # Manages SSTable compaction to reduce disk usage and improve read performance
  class CompactionManager
    @db_path : String
    @sstables : Array(SSTable)
    @mutex : Mutex
    @running : Bool
    @compaction_fiber : Fiber?

    def initialize(@db_path : String)
      @sstables = [] of SSTable
      @mutex = Mutex.new
      @running = false
    end

    def add_sstable(sstable : SSTable)
      @mutex.synchronize do
        @sstables << sstable
      end
    end

    def get_sstables : Array(SSTable)
      @mutex.synchronize do
        @sstables.dup
      end
    end

    def start_background_compaction(threshold : Int32 = 4)
      return if @running
      @running = true
      
      @compaction_fiber = spawn do
        while @running
          sleep 10.seconds
          compact_if_needed(threshold) if @running
        end
      end
    end

    def stop_background_compaction
      @running = false
      sleep 0.1.seconds
    end

    def compact_if_needed(threshold : Int32)
      tables_to_compact = @mutex.synchronize do
        @sstables.size >= threshold ? @sstables.dup : nil
      end
      
      return unless tables_to_compact
      
      compact(tables_to_compact)
    end

    def compact(tables : Array(SSTable))
      return if tables.empty?

      merged = {} of String => Tuple(Value?, Bool, Int64)
      
      tables.each do |table|
        table.scan do |key, value, deleted, timestamp|
          existing = merged[key]?
          if existing.nil? || timestamp > existing[2]
            merged[key] = {value, deleted, timestamp}
          end
        end
      end
      
      entries = merged.compact_map do |key, (value, deleted, timestamp)|
        deleted ? nil : {key, value, deleted, timestamp}
      end
      
      return if entries.empty?
      
      timestamp = Time.utc.to_unix_ms
      new_path = File.join(@db_path, "sstable_#{timestamp}.sst")
      SSTable.create(new_path, entries)
      new_table = SSTable.new(new_path)
      
      @mutex.synchronize do
        tables.each do |table|
          @sstables.delete(table)
          table.close
          File.delete(table.@path) rescue nil
        end
        @sstables << new_table
      end
    end

    def close
      stop_background_compaction
      @mutex.synchronize do
        @sstables.each(&.close)
        @sstables.clear
      end
    end
  end
end
