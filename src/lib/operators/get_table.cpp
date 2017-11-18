#include "get_table.hpp"

#include <memory>
#include <string>

namespace opossum {

GetTable::GetTable(const std::string& name) {
  this->name_of_table = name;
  has_table = storage_manager.has_table(name);
  if (has_table) this->table_to_retrieve = storage_manager.get_table(name);
}

std::shared_ptr<const Table> GetTable::_on_execute() {
  if (has_table)
    return table_to_retrieve;
  else
    throw std::runtime_error("Table does not exist");
}

const std::string& GetTable::table_name() const { return name_of_table; }
}  // namespace opossum
