require "spec"
require "../src/star"

# Helper to recursively remove directories
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
