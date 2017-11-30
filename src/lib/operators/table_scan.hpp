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
  class TemplatedTableScanBase;
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan();

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  template<typename T>
  class TemplatedTableScan;
  std::shared_ptr<const Table> _on_execute() override;

 private:
  ColumnID _col_id;
  ScanType _type_of_scan;
  AllTypeVariant _value_to_find;
  // if the column to scan is a reference column, the following member holds the referenced table to use for result
  // (the RowIDs reference chunks in original table, not in reference table)
  std::optional<std::shared_ptr<const Table>> referenced_table = std::nullopt;
};

}  // namespace opossum
