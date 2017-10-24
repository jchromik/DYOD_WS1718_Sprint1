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

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name) != 0; }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  for (const auto &_table : _tables) {
    names.push_back(_table.first);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  printHeader(out);
  for (const auto &_table : _tables) {
    printTableInformation(out, _table.first, _table.second);
  }
}

void StorageManager::printTableInformation(std::ostream& out, const std::string& name,
                                           const std::shared_ptr<Table>& table) const {
  std::stringstream line;
  line << name << " (" << table->col_count() << ", " << table->row_count() << ", " << table->chunk_count() << ")\n";
  out.write(line.str().c_str(), line.str().size());
}

void StorageManager::printHeader(std::ostream& out) const {
  std::string header = "Table Name (#Columns, #Rows, #Chunks)\n";
  out.write(header.c_str(), header.size());
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
