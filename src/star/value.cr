module StarDB
  # Represents a typed value that can be stored in the database
  struct Value
    enum Type : UInt8
      Nil
      Bool
      Int8
      Int16
      Int32
      Int64
      UInt8
      UInt16
      UInt32
      UInt64
      Float32
      Float64
      String
      Bytes
    end

    getter type : Type
    @data : Bytes

    def initialize(@type : Type, @data : Bytes)
    end

    # Create a value from various types
    def self.from(value : Nil)
      new(Type::Nil, Bytes.empty)
    end

    def self.from(value : Bool)
      new(Type::Bool, Bytes[value ? 1_u8 : 0_u8])
    end

    def self.from(value : Int8)
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      new(Type::Int8, io.to_slice)
    end

    def self.from(value : Int16)
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      new(Type::Int16, io.to_slice)
    end

    def self.from(value : Int32)
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      new(Type::Int32, io.to_slice)
    end

    def self.from(value : Int64)
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      new(Type::Int64, io.to_slice)
    end

    def self.from(value : UInt8)
      new(Type::UInt8, Bytes[value])
    end

    def self.from(value : UInt16)
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      new(Type::UInt16, io.to_slice)
    end

    def self.from(value : UInt32)
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      new(Type::UInt32, io.to_slice)
    end

    def self.from(value : UInt64)
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      new(Type::UInt64, io.to_slice)
    end

    def self.from(value : Float32)
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      new(Type::Float32, io.to_slice)
    end

    def self.from(value : Float64)
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      new(Type::Float64, io.to_slice)
    end

    def self.from(value : String)
      new(Type::String, value.to_slice)
    end

    def self.from(value : Bytes)
      new(Type::Bytes, value)
    end

    def serialize(io : IO)
      io.write_byte(@type.value)
      io.write_bytes(@data.size.to_u32, IO::ByteFormat::LittleEndian)
      io.write(@data)
    end

    def self.deserialize(io : IO) : Value
      type = Type.new(io.read_byte.not_nil!)
      size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      data = Bytes.new(size)
      io.read_fully(data)
      new(type, data)
    end

    def as_nil : Nil
      raise "Type mismatch" unless @type == Type::Nil
      nil
    end

    def as_bool : Bool
      raise "Type mismatch" unless @type == Type::Bool
      @data[0] == 1_u8
    end

    def as_i8 : Int8
      raise "Type mismatch" unless @type == Type::Int8
      IO::Memory.new(@data).read_bytes(Int8, IO::ByteFormat::LittleEndian)
    end

    def as_i16 : Int16
      raise "Type mismatch" unless @type == Type::Int16
      IO::Memory.new(@data).read_bytes(Int16, IO::ByteFormat::LittleEndian)
    end

    def as_i32 : Int32
      raise "Type mismatch" unless @type == Type::Int32
      IO::Memory.new(@data).read_bytes(Int32, IO::ByteFormat::LittleEndian)
    end

    def as_i64 : Int64
      raise "Type mismatch" unless @type == Type::Int64
      IO::Memory.new(@data).read_bytes(Int64, IO::ByteFormat::LittleEndian)
    end

    def as_u8 : UInt8
      raise "Type mismatch" unless @type == Type::UInt8
      @data[0]
    end

    def as_u16 : UInt16
      raise "Type mismatch" unless @type == Type::UInt16
      IO::Memory.new(@data).read_bytes(UInt16, IO::ByteFormat::LittleEndian)
    end

    def as_u32 : UInt32
      raise "Type mismatch" unless @type == Type::UInt32
      IO::Memory.new(@data).read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    end

    def as_u64 : UInt64
      raise "Type mismatch" unless @type == Type::UInt64
      IO::Memory.new(@data).read_bytes(UInt64, IO::ByteFormat::LittleEndian)
    end

    def as_f32 : Float32
      raise "Type mismatch" unless @type == Type::Float32
      IO::Memory.new(@data).read_bytes(Float32, IO::ByteFormat::LittleEndian)
    end

    def as_f64 : Float64
      raise "Type mismatch" unless @type == Type::Float64
      IO::Memory.new(@data).read_bytes(Float64, IO::ByteFormat::LittleEndian)
    end

    def as_s : String
      raise "Type mismatch" unless @type == Type::String
      String.new(@data)
    end

    def as_bytes : Bytes
      raise "Type mismatch" unless @type == Type::Bytes
      @data
    end

    def size : Int32
      1 + 4 + @data.size
    end
  end
end
