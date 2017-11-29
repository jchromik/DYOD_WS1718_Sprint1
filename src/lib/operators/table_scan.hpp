#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <storage/reference_column.hpp>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "type_cast.hpp"

namespace opossum {

class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan();

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  ColumnID col_id;
  ScanType type_of_scan;
  AllTypeVariant value_to_find;
};

}  // namespace opossum
