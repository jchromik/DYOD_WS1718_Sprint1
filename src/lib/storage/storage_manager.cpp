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
  _tables.insert({name, table});
}

void StorageManager::drop_table(const std::string& name) {
  _check_table_existence(name);
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  _check_table_existence(name);
  return _tables.at(name);
}

bool StorageManager::has_table(const std::string& name) const { return _tables.find(name) != _tables.end(); }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  for (const auto& table_entry : _tables) {
    names.push_back(table_entry.first);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  _print_header(out);
  for (const auto& _table : _tables) {
    _print_table_information(out, _table.first, _table.second);
  }
}

void StorageManager::_print_table_information(std::ostream& out, const std::string& name,
                                              const std::shared_ptr<Table>& table) const {
  out << name << " (" << table->col_count() << ", " << table->row_count() << ", " << table->chunk_count() << ")\n";
}

void StorageManager::_print_header(std::ostream& out) const {
  std::string header = "Table Name (#Columns, #Rows, #Chunks)\n";
  out.write(header.c_str(), header.size());
}

// write the new instance returned by StorageManager() to the address returned by get()
void StorageManager::reset() {
  get() = StorageManager();
}

void StorageManager::_check_table_existence(const std::string& name) const {
  if (!has_table(name)) {
    throw std::runtime_error("No such table");
  }
}

}  // namespace opossum
