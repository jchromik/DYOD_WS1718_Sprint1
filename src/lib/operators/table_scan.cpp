#include <storage/table.hpp>
#include "table_scan.hpp"

namespace opossum {


TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value) : AbstractOperator(in), col_id(column_id),
                                                                                    type_of_scan(scan_type),
                                                                                    value_to_find(search_value) {

}

TableScan::~TableScan() {

}

ColumnID TableScan::column_id() const {
  return col_id;
}

ScanType TableScan::scan_type() const {
  return type_of_scan;
}

const AllTypeVariant &TableScan::search_value() const {
  return value_to_find;
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  std::shared_ptr<const Table> operator_result = _input_table_left();

  auto result_table = std::make_shared<Table>(operator_result->chunk_size());
  for (ColumnID col_id = ColumnID{0}; col_id < operator_result->col_count(); ++col_id) {
    const std::string col_type = operator_result->column_type(col_id);
    const std::string col_name = operator_result->column_name(col_id);
    result_table->add_column_definition(col_name, col_type);
  }

  // TODO: continue here

  return nullptr;
}
}  // namespace opossum
