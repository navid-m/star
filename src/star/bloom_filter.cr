require "digest/sha256"

module StarDB
  # Bloom filter for fast negative lookups
  class BloomFilter
    @bits : Bytes
    @size : Int32
    @hash_count : Int32

    def initialize(@size : Int32, @hash_count : Int32 = 3)
      @bits = Bytes.new((@size + 7) // 8, 0_u8)
    end

    def initialize(@bits : Bytes, @hash_count : Int32 = 3)
      @size = @bits.size * 8
    end

    def add(key : String)
      @hash_count.times do |i|
        pos = hash(key, i) % @size
        byte_pos = pos // 8
        bit_pos = pos % 8
        @bits[byte_pos] |= (1_u8 << bit_pos)
      end
    end

    def might_contain?(key : String) : Bool
      @hash_count.times do |i|
        pos = hash(key, i) % @size
        byte_pos = pos // 8
        bit_pos = pos % 8
        return false if (@bits[byte_pos] & (1_u8 << bit_pos)) == 0
      end
      true
    end

    def serialize(io : IO)
      io.write_bytes(@size, IO::ByteFormat::LittleEndian)
      io.write_bytes(@hash_count, IO::ByteFormat::LittleEndian)
      io.write(@bits)
    end

    def self.deserialize(io : IO) : BloomFilter
      size = io.read_bytes(Int32, IO::ByteFormat::LittleEndian)
      hash_count = io.read_bytes(Int32, IO::ByteFormat::LittleEndian)
      bits = Bytes.new((size + 7) // 8)
      io.read_fully(bits)
      new(bits, hash_count)
    end

    private def hash(key : String, seed : Int32) : Int32
      digest = Digest::SHA256.digest("#{seed}:#{key}")
      (digest[0].to_i32 << 24) | (digest[1].to_i32 << 16) | (digest[2].to_i32 << 8) | digest[3].to_i32
    end

    def self.optimal_size(expected_items : Int32, false_positive_rate : Float64 = 0.01) : Int32
      (-expected_items * Math.log(false_positive_rate) / (Math.log(2) ** 2)).ceil.to_i32
    end

    def self.optimal_hash_count(size : Int32, expected_items : Int32) : Int32
      ((size / expected_items) * Math.log(2)).ceil.to_i32.clamp(1, 10)
    end
  end
end
