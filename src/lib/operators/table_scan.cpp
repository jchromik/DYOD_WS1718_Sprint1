#include <storage/table.hpp>
#include <resolve_type.hpp>
#include "table_scan.hpp"

namespace opossum {

class TemplatedTableScanBase {
 public:
  virtual std::shared_ptr<const Table> _on_execute() = 0;
};

template<typename T>
class TemplatedTableScan : TemplatedTableScanBase {
  public:
  TemplatedTableScan(std::shared_ptr<const Table>& table_to_scan, const ColumnID& column_id,
                     const ScanType& scan_type, const AllTypeVariant& search_value,
                     const TableScan& table_scan) : table_to_scan(table_to_scan),
                                                    col_id(column_id),
                                                    type_of_scan(scan_type),
                                                    value_to_find(search_value),
                                                    table_scan(table_scan) {

  }

//void TemplatedTableScan::process_value_column(const ValueColumn<T>& value_column) {
//  const std::vector<T> &values = value_column.values();
//  for (ChunkOffset offset = 0; offset < value_column.size(); ++offset) {
//    const T &value = values.at(offset);
//    if (matches_scan_type(value, value_to_find)) {
//      // TODO: add to result table
//    }
//  }
//  // TODO
//}
//
//void TemplatedTableScan::process_dictionary_column(const DictionaryColumn& dictionary_column) {
//  // TODO
//}
//
//void TemplatedTableScan::process_reference_column(const ReferenceColumn& reference_column) {
//  // TODO
//}

  protected:
  std::shared_ptr<const Table> _on_execute() override {

    // TODO: extract to method, but if I do so I always get an error...
    auto result_table = std::make_shared<Table>(table_to_scan->chunk_size());
    for (ColumnID col_id = ColumnID{0}; col_id < table_to_scan->col_count(); ++col_id) {
      const std::string col_type = table_to_scan->column_type(col_id);
      const std::string col_name = table_to_scan->column_name(col_id);
      result_table->add_column_definition(col_name, col_type);
    }

    for (ChunkID chunk_id = ChunkID{0}; chunk_id < table_to_scan->chunk_count(); ++chunk_id) {
      const Chunk &chunk = table_to_scan->get_chunk(chunk_id);
      const std::shared_ptr<BaseColumn> column = chunk.get_column(col_id);

//    column->process_column_in_table_scan(table_scan);
      process_column(chunk_id, column);
    }

    // can we just add to the first chunk?
    Chunk& chunk = result_table->get_chunk(ChunkID{0});
    for (ColumnID column_id{0}; column_id < result_table->col_count(); ++column_id) {
      auto reference_column = std::make_shared<ReferenceColumn>(table_to_scan, column_id, result);
      chunk.add_column(reference_column);
    }

    return result_table;
  }

  bool matches_scan_type(const T &left, const T &right) {
    switch (type_of_scan) {
      case ScanType::OpEquals:
        return left == right;
      case ScanType::OpNotEquals:
        return left != right;
      case ScanType::OpLessThan:
        return left < right;
      case ScanType::OpLessThanEquals:
        return left <= right;
      case ScanType::OpGreaterThan:
        return left > right;
      case ScanType::OpGreaterThanEquals:
        return left >= right;
      case ScanType::OpBetween: // TODO: added by me because it was used in a test. check if still necessary before submitting the sprint
      default:
        throw std::runtime_error("The ScanType is not implemented");
    }
  }

  void process_column(const ChunkID &chunk_id, const std::shared_ptr<BaseColumn> column) {
    auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(column);
    if (value_column) {
      return process_value_column(chunk_id, value_column);
    }

    auto dictionary_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(column);
    if (dictionary_column) {
      return process_dictionary_column(chunk_id, dictionary_column);
    }

    auto reference_column = std::dynamic_pointer_cast<ReferenceColumn>(column);
    if (reference_column) {
      return process_reference_column(chunk_id, reference_column);
    }

    throw std::runtime_error("Unknown column type");
  }

  void process_value_column(const ChunkID &chunk_id, const std::shared_ptr<ValueColumn<T>> &value_column) {
    const std::vector<T> &values = value_column->values();
    for (ChunkOffset offset = 0; offset < value_column->size(); ++offset) {
      const T &value = values.at(offset);
      const T &value_to_find_t = type_cast<T>(value_to_find);
      if (matches_scan_type(value, value_to_find_t)) {
        result.emplace_back(RowID{chunk_id, offset});
      }
    }
  }

  void process_dictionary_column(const ChunkID &chunk_id, std::shared_ptr<DictionaryColumn<T>> dictionary_column) {
    const ValueID& pos_of_value_to_find = dictionary_column->lower_bound(value_to_find);
    for (ChunkOffset offset = 0; offset < dictionary_column->size(); ++offset) {
      const ValueID& value_id = dictionary_column->attribute_vector()->get(offset);
      if (matches_scan_type(value_id, pos_of_value_to_find)) {
        result.emplace_back(RowID{chunk_id, offset});
      }
    }
  }

  void process_reference_column(const ChunkID &chunk_id, const std::shared_ptr<ReferenceColumn> &reference_column) {
    auto table = reference_column->referenced_table();

    const std::shared_ptr<const PosList> pos_list = reference_column->pos_list();
    for (auto iterator = pos_list->cbegin(); iterator != pos_list->cend(); ++iterator) {
      const RowID row_id = *iterator;
      const Chunk& chunk = table->get_chunk(row_id.chunk_id);
      auto reference_chunk_column = chunk.get_column(reference_column->referenced_column_id());

      process_referenced_column_by_type(reference_chunk_column, row_id);
    }
  }

  void process_referenced_column_by_type(const std::shared_ptr<ReferenceColumn> &reference_column, const RowID &row_id) {
    auto value_column_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(reference_column);
    if (value_column_ptr) {
      return process_referenced_value_column(value_column_ptr, row_id);
    }

    auto dictionary_column_ptr = std::dynamic_pointer_cast<DictionaryColumn<T>>(reference_column);
    if (dictionary_column_ptr) {
      return process_referenced_dictionary_column(dictionary_column_ptr, row_id);
    }

    throw std::runtime_error("Unknown referenced column type");
  }

  void process_referenced_value_column(const std::shared_ptr<ValueColumn<T>> &value_column, const RowID &row_id) {
    const auto& value = value_column->values()[row_id.chunk_offset];
    if (matches_scan_type(value, value_to_find)) {
      // TODO: add to result table
    }
  }

  void process_referenced_dictionary_column(const std::shared_ptr<DictionaryColumn<T>> &dictionary_column, const RowID &row_id) {
    const auto& value_id = dictionary_column->attribute_vector()->get(row_id.chunk_offset);
    const auto& value = (*dictionary_column->dictionary()).at(value_id);
    if (matches_scan_type(value, value_to_find)) {
      result.emplace_back(row_id);
    }
  }

  std::shared_ptr<const Table> table_to_scan;
  PosList result;
  const ColumnID col_id;
  const ScanType type_of_scan;
  const AllTypeVariant value_to_find;
  const TableScan &table_scan;
};

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value) : AbstractOperator(in),
                                                                                    col_id(column_id),
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
  std::shared_ptr<const Table> table_to_scan = _input_table_left();

//  templated_table_scan = make_shared_by_column_type<TemplatedTableScan>(table_to_scan->column_type(col_id),
  auto templated_table_scan = make_shared_by_column_type<TemplatedTableScanBase, TemplatedTableScan>(table_to_scan->column_type(col_id),
                                                                                                     table_to_scan,
                                                                                                     col_id,
                                                                                                     type_of_scan,
                                                                                                     value_to_find,
                                                                                                     *this);

  return templated_table_scan->_on_execute();

}

//template<typename T>
//void TableScan::process_value_column(const ValueColumn<T>& value_column) {
//  templated_table_scan->process_value_column(value_column);
//}
//
//void TableScan::process_dictionary_column(const DictionaryColumn& dictionary_column) {
//  templated_table_scan->process_dictionary_column(dictionary_column);
//}
//
//void TableScan::process_reference_column(const ReferenceColumn& reference_column) {
//  templated_table_scan->process_reference_column(reference_column);
//}

}  // namespace opossum
