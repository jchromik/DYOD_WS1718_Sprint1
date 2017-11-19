#include "table_scan.hpp"

namespace opossum {


TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value) : in(in), col_id(column_id),
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
  in->execute();
  std::shared_ptr<const Table> operator_result = in->get_output();

  // TODO: continue here

  return nullptr;
}
}  // namespace opossum
