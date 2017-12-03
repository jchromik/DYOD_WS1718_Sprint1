#include "get_table.hpp"

#include <memory>
#include <string>

namespace opossum {

GetTable::GetTable(const std::string& name) { _name_of_table = name; }

std::shared_ptr<const Table> GetTable::_on_execute() {
  StorageManager& storage_manager = StorageManager::get();
  if (storage_manager.has_table(_name_of_table)) {
    return storage_manager.get_table(_name_of_table);
  } else {
    throw std::runtime_error("Table does not exist");
  }
}

const std::string& GetTable::table_name() const { return _name_of_table; }
}  // namespace opossum
