#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager _singleton;
  return _singleton;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  if (has_table(name)) {
    throw std::runtime_error("Table already exists");
  }
  _tables.insert(std::pair<std::string, std::shared_ptr<Table>>(name, table));
}

void StorageManager::drop_table(const std::string& name) {
  check_table_existence(name);
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  check_table_existence(name);
  return _tables.at(name);
}

bool StorageManager::has_table(const std::string& name) const {
  return _tables.count(name) != 0;
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  for(auto it = _tables.begin(); it != _tables.end(); ++it) {
    names.push_back(it->first);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  for(auto it = _tables.begin(); it != _tables.end(); ++it) {
    std::string name = it->first;
    std::shared_ptr<Table> table = it->second;
    std::cout << "("
     + name + ", "
     + std::to_string(table->col_count()) + ", "
     + std::to_string(table->row_count()) + ", "
     + std::to_string(table->chunk_count()) + ")\n";
  }
}

void StorageManager::reset() {
  // write the new instance returned by StorageManager() to the address returned by get()
  get() = StorageManager();
}

void StorageManager::check_table_existence(const std::string& name) const {
  if (!has_table(name)) {
    throw std::runtime_error("No such table");
  }
}

}  // namespace opossum
