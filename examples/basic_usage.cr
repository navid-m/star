require "../src/stardb"

db = StarDB::Database.new("my_database")

db.put("name", "Alice")
db.put("age", 30)
db.put("score", 95.5_f64)
db.put("active", true)
db.put("data", Bytes[1, 2, 3, 4, 5])

name = db.get("name").not_nil!.as_s
age = db.get("age").not_nil!.as_i32
score = db.get("score").not_nil!.as_f64
active = db.get("active").not_nil!.as_bool

puts "Name: #{name}"
puts "Age: #{age}"
puts "Score: #{score}"
puts "Active: #{active}"

db.put("age", 31)
puts "Updated age: #{db.get("age").not_nil!.as_i32}"

db.delete("data")
puts "Data after delete: #{db.get("data").inspect}"

puts "\nStoring multiple keys..."
('a'..'z').each_with_index do |char, i|
  db.put("key_#{char}", i)
end

puts "\nScanning keys from 'key_m' to 'key_s':"
db.scan("key_m", "key_s") do |key, value|
  puts "  #{key} => #{value.not_nil!.as_i32}"
end

db.close

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

rm_rf("my_database")
