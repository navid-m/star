require "./value"

module StarDB
  # In-memory sorted table using a skip list for O(log n) operations
  class MemTable
    MAX_LEVEL = 16
    PROBABILITY = 0.5

    private class Node
      property key : String
      property value : Value?
      property deleted : Bool
      property timestamp : Int64
      property forward : Array(Node?)

      def initialize(@key : String, @value : Value?, @deleted : Bool, @timestamp : Int64, level : Int32)
        @forward = Array(Node?).new(level + 1, nil)
      end
    end

    @head : Node
    @level : Int32
    @size : Int32
    @byte_size : Int64
    @mutex : Mutex

    def initialize
      @head = Node.new("", nil, false, 0_i64, MAX_LEVEL)
      @level = 0
      @size = 0
      @byte_size = 0_i64
      @mutex = Mutex.new
    end

    def put(key : String, value : Value)
      @mutex.synchronize do
        update = Array(Node?).new(MAX_LEVEL + 1, nil)
        current = @head

        (@level).downto(0) do |i|
          while (next_node = current.forward[i]) && next_node.key < key
            current = next_node
          end
          update[i] = current
        end

        current = current.forward[0]

        if current && current.key == key
          old_size = current.value.try(&.size) || 0
          current.value = value
          current.deleted = false
          current.timestamp = Time.utc.to_unix_ms
          @byte_size += value.size - old_size
        else
          new_level = random_level
          if new_level > @level
            ((@level + 1)..new_level).each do |i|
              update[i] = @head
            end
            @level = new_level
          end

          new_node = Node.new(key, value, false, Time.utc.to_unix_ms, new_level)
          (0..new_level).each do |i|
            new_node.forward[i] = update[i].not_nil!.forward[i]
            update[i].not_nil!.forward[i] = new_node
          end

          @size += 1
          @byte_size += key.bytesize + value.size + 16
        end
      end
    end

    def get(key : String) : Value?
      @mutex.synchronize do
        current = @head
        (@level).downto(0) do |i|
          while (next_node = current.forward[i]) && next_node.key < key
            current = next_node
          end
        end

        current = current.forward[0]
        if current && current.key == key && !current.deleted
          current.value
        else
          nil
        end
      end
    end

    def delete(key : String)
      @mutex.synchronize do
        update = Array(Node?).new(MAX_LEVEL + 1, nil)
        current = @head

        (@level).downto(0) do |i|
          while (next_node = current.forward[i]) && next_node.key < key
            current = next_node
          end
          update[i] = current
        end

        current = current.forward[0]

        if current && current.key == key
          current.deleted = true
          current.timestamp = Time.utc.to_unix_ms
          old_size = current.value.try(&.size) || 0
          current.value = nil
          @byte_size -= old_size
        end
      end
    end

    def each(&block : String, Value?, Bool, Int64 ->)
      @mutex.synchronize do
        current = @head.forward[0]
        while current
          yield current.key, current.value, current.deleted, current.timestamp
          current = current.forward[0]
        end
      end
    end

    def size : Int32
      @size
    end

    def byte_size : Int64
      @byte_size
    end

    def clear
      @mutex.synchronize do
        @head = Node.new("", nil, false, 0_i64, MAX_LEVEL)
        @level = 0
        @size = 0
        @byte_size = 0_i64
      end
    end

    private def random_level : Int32
      level = 0
      while Random.rand < PROBABILITY && level < MAX_LEVEL
        level += 1
      end
      level
    end
  end
end
