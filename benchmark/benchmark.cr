require "../src/stardb"
require "benchmark"

def rm_rf(path : String)
  return unless Dir.exists?(path) || File.exists?(path)
  if Dir.exists?(path)
    Dir.each_child(path) do |entry|
      full_path = File.join(path, entry)
      if Dir.exists?(full_path)
        rm_rf(full_path)
      else
        File.delete(full_path)
      end
    end
    Dir.delete(path)
  else
    File.delete(path)
  end
end

NUM_OPERATIONS = 100_000
KEY_SIZE = 16
VALUE_SIZE = 100

def random_string(size : Int32) : String
  String.build do |str|
    size.times { str << ('a'..'z').to_a.sample }
  end
end

def benchmark_sequential_writes(db : StarDB::Database, count : Int32)
  count.times do |i|
    key = "key_#{i.to_s.rjust(10, '0')}"
    value = "value_#{i}_#{random_string(VALUE_SIZE)}"
    db.put(key, value)
  end
end

def benchmark_random_writes(db : StarDB::Database, count : Int32)
  count.times do
    key = "key_#{Random.rand(count).to_s.rjust(10, '0')}"
    value = "value_#{random_string(VALUE_SIZE)}"
    db.put(key, value)
  end
end

def benchmark_sequential_reads(db : StarDB::Database, count : Int32)
  count.times do |i|
    key = "key_#{i.to_s.rjust(10, '0')}"
    db.get(key)
  end
end

def benchmark_random_reads(db : StarDB::Database, count : Int32)
  count.times do
    key = "key_#{Random.rand(count).to_s.rjust(10, '0')}"
    db.get(key)
  end
end

def benchmark_mixed_workload(db : StarDB::Database, count : Int32)
  count.times do
    operation = Random.rand(100)
    key = "key_#{Random.rand(count).to_s.rjust(10, '0')}"
    
    if operation < 50 # 50% reads
      db.get(key)
    elsif operation < 90 # 40% writes
      value = "value_#{random_string(VALUE_SIZE)}"
      db.put(key, value)
    else # 10% deletes
      db.delete(key)
    end
  end
end

def benchmark_range_scans(db : StarDB::Database, count : Int32, scan_size : Int32)
  (count // scan_size).times do |i|
    start_key = "key_#{(i * scan_size).to_s.rjust(10, '0')}"
    end_key = "key_#{((i + 1) * scan_size).to_s.rjust(10, '0')}"
    
    results = [] of String
    db.scan(start_key, end_key) do |key, value|
      results << key
    end
  end
end

puts "=" * 80
puts "StarDB Performance Benchmark"
puts "=" * 80
puts "Operations: #{NUM_OPERATIONS}"
puts "Key Size: #{KEY_SIZE} bytes"
puts "Value Size: #{VALUE_SIZE} bytes"
puts "=" * 80
puts

rm_rf("tmp/benchmark_db")
Dir.mkdir_p("tmp/benchmark_db")

db = StarDB::Database.new("tmp/benchmark_db", sync_on_write: false)

puts "Sequential Writes:"
time = Benchmark.measure do
  benchmark_sequential_writes(db, NUM_OPERATIONS)
end
ops_per_sec = NUM_OPERATIONS / time.real
puts "  Time: #{time.real.round(3)}s"
puts "  Throughput: #{ops_per_sec.round(0)} ops/sec"
puts "  Latency: #{(time.real * 1_000_000 / NUM_OPERATIONS).round(2)} μs/op"
puts

puts "Sequential Reads:"
time = Benchmark.measure do
  benchmark_sequential_reads(db, NUM_OPERATIONS)
end
ops_per_sec = NUM_OPERATIONS / time.real
puts "  Time: #{time.real.round(3)}s"
puts "  Throughput: #{ops_per_sec.round(0)} ops/sec"
puts "  Latency: #{(time.real * 1_000_000 / NUM_OPERATIONS).round(2)} μs/op"
puts

puts "Random Writes:"
time = Benchmark.measure do
  benchmark_random_writes(db, NUM_OPERATIONS)
end
ops_per_sec = NUM_OPERATIONS / time.real
puts "  Time: #{time.real.round(3)}s"
puts "  Throughput: #{ops_per_sec.round(0)} ops/sec"
puts "  Latency: #{(time.real * 1_000_000 / NUM_OPERATIONS).round(2)} μs/op"
puts

puts "Random Reads:"
time = Benchmark.measure do
  benchmark_random_reads(db, NUM_OPERATIONS)
end
ops_per_sec = NUM_OPERATIONS / time.real
puts "  Time: #{time.real.round(3)}s"
puts "  Throughput: #{ops_per_sec.round(0)} ops/sec"
puts "  Latency: #{(time.real * 1_000_000 / NUM_OPERATIONS).round(2)} μs/op"
puts

puts "Mixed Workload (50% read, 40% write, 10% delete):"
time = Benchmark.measure do
  benchmark_mixed_workload(db, NUM_OPERATIONS)
end
ops_per_sec = NUM_OPERATIONS / time.real
puts "  Time: #{time.real.round(3)}s"
puts "  Throughput: #{ops_per_sec.round(0)} ops/sec"
puts "  Latency: #{(time.real * 1_000_000 / NUM_OPERATIONS).round(2)} μs/op"
puts

scan_size = 100
puts "Range Scans (#{scan_size} keys per scan):"
time = Benchmark.measure do
  benchmark_range_scans(db, NUM_OPERATIONS, scan_size)
end
num_scans = NUM_OPERATIONS // scan_size
ops_per_sec = num_scans / time.real
puts "  Time: #{time.real.round(3)}s"
puts "  Throughput: #{ops_per_sec.round(0)} scans/sec"
puts "  Latency: #{(time.real * 1000 / num_scans).round(2)} ms/scan"
puts

puts "Data Types Performance:"
types_count = 10_000
time = Benchmark.measure do
  types_count.times do |i|
    db.put("int_#{i}", i)
    db.put("float_#{i}", i.to_f64 * 3.14)
    db.put("bool_#{i}", i % 2 == 0)
    db.put("string_#{i}", "value_#{i}")
  end
end
total_ops = types_count * 4
ops_per_sec = total_ops / time.real
puts "  Time: #{time.real.round(3)}s"
puts "  Throughput: #{ops_per_sec.round(0)} ops/sec"
puts

db.close

db_size = Dir.glob("tmp/benchmark_db/*").sum { |f| File.size(f) }
puts "=" * 80
puts "Database Size: #{(db_size / 1024.0 / 1024.0).round(2)} MB"
puts "=" * 80

rm_rf("tmp/benchmark_db")
