#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager* StorageManager::storageManager;
std::map<std::string, std::shared_ptr<Table>> tables;

StorageManager& StorageManager::get() {
  return *storageManager;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  tables.insert(std::make_pair(name, table));
}

void StorageManager::drop_table(const std::string& name) {
  check_table_existence(name);
  tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  check_table_existence(name);
  return tables.find(name)->second;
}

bool StorageManager::has_table(const std::string& name) const {
  return tables.find(name) != tables.end();
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  for(auto const& t : tables)
    table_names.push_back(t.first);
  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  std::string line_break = "\n";
  for (const std::string &t_name : table_names()) {
    out.write(t_name.c_str(), t_name.size());
    out.write(line_break.c_str(), line_break.size());
  }
  // Implementation goes here
}

void StorageManager::reset() {
// TODO: reset the storage manager itself instead of just clearing table
//  delete StorageManager::storageManager;
//  *StorageManager::storageManager = StorageManager();
  tables.clear();
}

void StorageManager::check_table_existence(const std::string& name) const {
  if (!has_table(name))
    throw std::runtime_error("No such table");
}

}  // namespace opossum
