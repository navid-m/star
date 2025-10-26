require "./value"

module StarDB
  # High-performance append-only memtable.
  # Writes are O(1), reads are O(n) though still are fast due to cache locality
  class MemTableV2
    struct Entry
      property key : String
      property value : Value?
      property deleted : Bool
      property timestamp : Int64

      def initialize(@key : String, @value : Value?, @deleted : Bool, @timestamp : Int64)
      end
    end

    @entries : Array(Entry)
    @byte_size : Int64
    @index : Hash(String, Int32)

    def initialize(capacity : Int32 = 10000)
      @entries = Array(Entry).new(capacity)
      @byte_size = 0_i64
      @index = Hash(String, Int32).new
    end

    def put(key : String, value : Value)
      timestamp = Time.utc.to_unix_ms
      
      if idx = @index[key]?
        old_entry = @entries[idx]
        old_size = old_entry.value.try(&.size) || 0
        @entries[idx] = Entry.new(key, value, false, timestamp)
        @byte_size += value.size - old_size
      else
        @index[key] = @entries.size
        @entries << Entry.new(key, value, false, timestamp)
        @byte_size += key.bytesize + value.size + 16
      end
    end

    def get(key : String) : Value?
      if idx = @index[key]?
        entry = @entries[idx]
        entry.deleted ? nil : entry.value
      else
        nil
      end
    end

    def delete(key : String)
      timestamp = Time.utc.to_unix_ms
      
      if idx = @index[key]?
        old_entry = @entries[idx]
        old_size = old_entry.value.try(&.size) || 0
        @entries[idx] = Entry.new(key, nil, true, timestamp)
        @byte_size -= old_size
      else
        @index[key] = @entries.size
        @entries << Entry.new(key, nil, true, timestamp)
        @byte_size += key.bytesize + 16
      end
    end

    def each(&block : String, Value?, Bool, Int64 ->)
      @entries.each do |entry|
        yield entry.key, entry.value, entry.deleted, entry.timestamp
      end
    end

    def size : Int32
      @entries.size
    end

    def byte_size : Int64
      @byte_size
    end

    def clear
      @entries.clear
      @index.clear
      @byte_size = 0_i64
    end

    def sorted_entries : Array(Entry)
      @entries.sort_by { |e| e.key }
    end
  end

  # Immutable sorted memtable for fast binary search
  class ImmutableMemTable
    @entries : Array(MemTableV2::Entry)

    def initialize(entries : Array(MemTableV2::Entry))
      @entries = entries.sort_by { |e| e.key }
    end

    def get(key : String) : Value?
      idx = binary_search(key)
      return nil if idx.nil?
      
      entry = @entries[idx]
      entry.deleted ? nil : entry.value
    end

    def each(&block : String, Value?, Bool, Int64 ->)
      @entries.each do |entry|
        yield entry.key, entry.value, entry.deleted, entry.timestamp
      end
    end

    def size : Int32
      @entries.size
    end

    private def binary_search(key : String) : Int32?
      left = 0
      right = @entries.size - 1

      while left <= right
        mid = (left + right) // 2
        mid_key = @entries[mid].key

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
  end
end
