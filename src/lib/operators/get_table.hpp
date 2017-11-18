#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class GetTable : public AbstractOperator {
 public:
  explicit GetTable(const std::string& name);

  const std::string& table_name() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  StorageManager& storage_manager = StorageManager::get();
  std::string name_of_table;
  bool has_table = false;
  std::shared_ptr<const Table> table_to_retrieve;
};
}  // namespace opossum
