require "./spec_helper"

describe StarDB do
  describe "Value" do
    it "serializes and deserializes integers" do
      val = StarDB::Value.from(42)
      io = IO::Memory.new
      val.serialize(io)
      io.rewind
      restored = StarDB::Value.deserialize(io)
      restored.as_i32.should eq(42)
    end

    it "serializes and deserializes strings" do
      val = StarDB::Value.from("hello world")
      io = IO::Memory.new
      val.serialize(io)
      io.rewind
      restored = StarDB::Value.deserialize(io)
      restored.as_s.should eq("hello world")
    end

    it "serializes and deserializes floats" do
      val = StarDB::Value.from(3.14_f64)
      io = IO::Memory.new
      val.serialize(io)
      io.rewind
      restored = StarDB::Value.deserialize(io)
      restored.as_f64.should be_close(3.14, 0.001)
    end
  end

  describe "BloomFilter" do
    it "correctly identifies added items" do
      bf = StarDB::BloomFilter.new(1000, 3)
      bf.add("key1")
      bf.add("key2")
      bf.might_contain?("key1").should be_true
      bf.might_contain?("key2").should be_true
    end

    it "has low false positive rate" do
      bf = StarDB::BloomFilter.new(10000, 3)
      100.times { |i| bf.add("key#{i}") }
      
      false_positives = 0
      1000.times do |i|
        false_positives += 1 if bf.might_contain?("notkey#{i}")
      end
      
      (false_positives.to_f / 1000).should be < 0.1
    end
  end

  describe "MemTable" do
    it "stores and retrieves values" do
      mt = StarDB::MemTable.new
      mt.put("key1", StarDB::Value.from(100))
      mt.put("key2", StarDB::Value.from("value2"))
      
      mt.get("key1").not_nil!.as_i32.should eq(100)
      mt.get("key2").not_nil!.as_s.should eq("value2")
    end

    it "handles updates" do
      mt = StarDB::MemTable.new
      mt.put("key1", StarDB::Value.from(100))
      mt.put("key1", StarDB::Value.from(200))
      
      mt.get("key1").not_nil!.as_i32.should eq(200)
    end

    it "handles deletes" do
      mt = StarDB::MemTable.new
      mt.put("key1", StarDB::Value.from(100))
      mt.delete("key1")
      
      mt.get("key1").should be_nil
    end

    it "maintains sorted order" do
      mt = StarDB::MemTable.new
      mt.put("c", StarDB::Value.from(3))
      mt.put("a", StarDB::Value.from(1))
      mt.put("b", StarDB::Value.from(2))
      
      keys = [] of String
      mt.each { |k, v, d, t| keys << k unless d }
      keys.should eq(["a", "b", "c"])
    end
  end

  describe "Database" do
    it "performs basic put and get operations" do
      rm_rf("tmp/test_db")
      Dir.mkdir_p("tmp/test_db")
      db = StarDB::Database.new("tmp/test_db")
      
      db.put("name", "Alice")
      db.put("age", 30)
      db.put("score", 95.5_f64)
      
      db.get("name").not_nil!.as_s.should eq("Alice")
      db.get("age").not_nil!.as_i32.should eq(30)
      db.get("score").not_nil!.as_f64.should be_close(95.5, 0.001)
      
      db.close
      rm_rf("tmp/test_db")
    end

    it "handles updates correctly" do
      rm_rf("tmp/test_db2")
      Dir.mkdir_p("tmp/test_db2")
      db = StarDB::Database.new("tmp/test_db2")
      
      db.put("counter", 1)
      db.put("counter", 2)
      db.put("counter", 3)
      
      db.get("counter").not_nil!.as_i32.should eq(3)
      
      db.close
      rm_rf("tmp/test_db2")
    end

    it "handles deletes correctly" do
      rm_rf("tmp/test_db3")
      Dir.mkdir_p("tmp/test_db3")
      db = StarDB::Database.new("tmp/test_db3")
      
      db.put("temp", "value")
      db.get("temp").should_not be_nil
      
      db.delete("temp")
      db.get("temp").should be_nil
      
      db.close
      rm_rf("tmp/test_db3")
    end

    it "persists data across restarts" do
      rm_rf("tmp/test_db4")
      Dir.mkdir_p("tmp/test_db4")
      
      db = StarDB::Database.new("tmp/test_db4")
      db.put("persistent", "data")
      db.close
      
      db2 = StarDB::Database.new("tmp/test_db4")
      db2.get("persistent").not_nil!.as_s.should eq("data")
      db2.close
      
      rm_rf("tmp/test_db4")
    end

    it "performs range scans" do
      rm_rf("tmp/test_db5")
      Dir.mkdir_p("tmp/test_db5")
      db = StarDB::Database.new("tmp/test_db5")
      
      db.put("a", 1)
      db.put("b", 2)
      db.put("c", 3)
      db.put("d", 4)
      db.put("e", 5)
      
      results = [] of Tuple(String, Int32)
      db.scan("b", "d") do |key, value|
        results << {key, value.not_nil!.as_i32}
      end
      
      results.should eq([{"b", 2}, {"c", 3}, {"d", 4}])
      
      db.close
      rm_rf("tmp/test_db5")
    end

    it "handles various data types" do
      rm_rf("tmp/test_db6")
      Dir.mkdir_p("tmp/test_db6")
      db = StarDB::Database.new("tmp/test_db6")
      
      db.put("bool", true)
      db.put("int8", 127_i8)
      db.put("int16", 32767_i16)
      db.put("int32", 2147483647)
      db.put("int64", 9223372036854775807_i64)
      db.put("uint8", 255_u8)
      db.put("uint16", 65535_u16)
      db.put("uint32", 4294967295_u32)
      db.put("float32", 3.14_f32)
      db.put("float64", 2.718281828_f64)
      db.put("string", "Hello, StarDB!")
      db.put("bytes", "binary data".to_slice)
      
      db.get("bool").not_nil!.as_bool.should be_true
      db.get("int8").not_nil!.as_i8.should eq(127_i8)
      db.get("int16").not_nil!.as_i16.should eq(32767_i16)
      db.get("int32").not_nil!.as_i32.should eq(2147483647)
      db.get("int64").not_nil!.as_i64.should eq(9223372036854775807_i64)
      db.get("uint8").not_nil!.as_u8.should eq(255_u8)
      db.get("uint16").not_nil!.as_u16.should eq(65535_u16)
      db.get("uint32").not_nil!.as_u32.should eq(4294967295_u32)
      db.get("float32").not_nil!.as_f32.should be_close(3.14_f32, 0.001)
      db.get("float64").not_nil!.as_f64.should be_close(2.718281828, 0.000001)
      db.get("string").not_nil!.as_s.should eq("Hello, StarDB!")
      String.new(db.get("bytes").not_nil!.as_bytes).should eq("binary data")
      
      db.close
      rm_rf("tmp/test_db6")
    end
  end
end
