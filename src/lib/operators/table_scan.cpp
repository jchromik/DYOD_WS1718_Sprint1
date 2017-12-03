#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "table_scan.hpp"

namespace opossum {

template <typename T>
bool matches_scan_type(const T& left, const T& right, ScanType type_of_scan) {
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
    default:
      throw std::runtime_error("The ScanType is not implemented");
  }
}

class TableScan::TemplatedTableScanBase {
 public:
  TemplatedTableScanBase() = default;

  virtual std::shared_ptr<PosList> _on_execute() = 0;
};

template <typename T>
class TableScan::TemplatedTableScan : public TableScan::TemplatedTableScanBase {
 public:
  explicit TemplatedTableScan(std::shared_ptr<const Table>& table_to_scan, const ColumnID& column_id,
                              const ScanType& scan_type, const AllTypeVariant& search_value, TableScan& table_scan)
      : TableScan::TemplatedTableScanBase{},
        _table_to_scan(table_to_scan),
        _col_id(column_id),
        _type_of_scan(scan_type),
        _value_to_find(type_cast<T>(search_value)),
        _table_scan(table_scan) {
    _result = std::make_shared<PosList>();
  }

 protected:
  std::shared_ptr<const Table> _table_to_scan;
  std::shared_ptr<PosList> _result;
  const ColumnID _col_id;
  const ScanType _type_of_scan;
  const T _value_to_find;
  TableScan& _table_scan;

  std::shared_ptr<PosList> _on_execute() override {
    for (ChunkID chunk_id = ChunkID{0}; chunk_id < _table_to_scan->chunk_count(); ++chunk_id) {
      const Chunk& chunk = _table_to_scan->get_chunk(chunk_id);
      const std::shared_ptr<BaseColumn> column = chunk.get_column(_col_id);

      _process_column(chunk_id, column);
    }

    return _result;
  }

  void _process_column(const ChunkID& chunk_id, const std::shared_ptr<BaseColumn> column) {
    auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(column);
    if (value_column) {
      return _process_value_column(chunk_id, value_column);
    }

    auto dictionary_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(column);
    if (dictionary_column) {
      return _process_dictionary_column(chunk_id, dictionary_column);
    }

    auto reference_column = std::dynamic_pointer_cast<ReferenceColumn>(column);
    if (reference_column) {
      return _process_reference_column(chunk_id, reference_column);
    }

    throw std::runtime_error("Unknown column type");
  }

  void _process_value_column(const ChunkID& chunk_id, const std::shared_ptr<ValueColumn<T>>& value_column) {
    const std::vector<T>& values = value_column->values();
    for (ChunkOffset offset = 0; offset < value_column->size(); ++offset) {
      const T& value = values[offset];
      if (matches_scan_type(value, _value_to_find, _type_of_scan)) {
        _result->push_back(RowID{chunk_id, offset});
      }
    }
  }

  void _process_dictionary_column(const ChunkID& chunk_id, std::shared_ptr<DictionaryColumn<T>> dictionary_column) {
    const ValueID& pos_of_value_to_find = dictionary_column->lower_bound(_value_to_find);
    if (pos_of_value_to_find == INVALID_VALUE_ID) {
      switch (_type_of_scan) {
        case ScanType::OpEquals:
        case ScanType::OpGreaterThan:
        case ScanType::OpGreaterThanEquals:
          return;
        case ScanType::OpNotEquals:
        case ScanType::OpLessThan:
        case ScanType::OpLessThanEquals:
          return _add_all(chunk_id, dictionary_column);
        default:
          throw std::runtime_error("The ScanType is not implemented");
      }
    } else if (dictionary_column->value_by_value_id(pos_of_value_to_find) != _value_to_find) {
      switch (_type_of_scan) {
        case ScanType::OpEquals:
        case ScanType::OpLessThanEquals:
          return;
        case ScanType::OpGreaterThan:
        case ScanType::OpNotEquals:
          return _add_all(chunk_id, dictionary_column);
        case ScanType::OpGreaterThanEquals:
        case ScanType::OpLessThan:
          return _add_matching(chunk_id, dictionary_column, pos_of_value_to_find);
        default:
          throw std::runtime_error("The ScanType is not implemented");
      }
    } else {
      return _add_matching(chunk_id, dictionary_column, pos_of_value_to_find);
    }
  }

  void _add_all(const ChunkID& chunk_id, std::shared_ptr<DictionaryColumn<T>> dictionary_column) {
    for (ChunkOffset offset = 0; offset < dictionary_column->size(); ++offset) {
      _result->push_back(RowID{chunk_id, offset});
    }
  }

  void _add_matching(const ChunkID& chunk_id, std::shared_ptr<DictionaryColumn<T>> dictionary_column,
                     const ValueID& pos_of_value_to_find) {
    for (ChunkOffset offset = 0; offset < dictionary_column->size(); ++offset) {
      const ValueID& value_id = dictionary_column->attribute_vector()->get(offset);
      if (matches_scan_type(value_id, pos_of_value_to_find, _type_of_scan)) {
        _result->push_back(RowID{chunk_id, offset});
      }
    }
  }

  void _process_reference_column(const ChunkID& chunk_id, const std::shared_ptr<ReferenceColumn>& reference_column) {
    if (!_table_scan._referenced_table.has_value()) {
      _table_scan._referenced_table = reference_column->referenced_table();
    } else if (_table_scan._referenced_table.value() != reference_column->referenced_table()) {
      throw std::runtime_error("The same column in different chunks references to different tables.");
    }

    const std::shared_ptr<const PosList> pos_list = reference_column->pos_list();
    for (auto& row_id : *pos_list) {
      const Chunk& chunk = _table_scan._referenced_table.value()->get_chunk(row_id.chunk_id);
      auto reference_chunk_column = chunk.get_column(reference_column->referenced_column_id());

      _process_referenced_column_by_type(reference_chunk_column, row_id);
    }
  }

  void _process_referenced_column_by_type(const std::shared_ptr<BaseColumn>& reference_column, const RowID& row_id) {
    auto value_column_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(reference_column);
    if (value_column_ptr) {
      return _process_referenced_value_column(value_column_ptr, row_id);
    }

    auto dictionary_column_ptr = std::dynamic_pointer_cast<DictionaryColumn<T>>(reference_column);
    if (dictionary_column_ptr) {
      return _process_referenced_dictionary_column(dictionary_column_ptr, row_id);
    }

    throw std::runtime_error("Unknown referenced column type");
  }

  void _process_referenced_value_column(const std::shared_ptr<ValueColumn<T>>& value_column, const RowID& row_id) {
    const auto& value = value_column->values()[row_id.chunk_offset];
    if (matches_scan_type(value, _value_to_find, _type_of_scan)) {
      _result->push_back(row_id);
    }
  }

  void _process_referenced_dictionary_column(const std::shared_ptr<DictionaryColumn<T>>& dictionary_column,
                                             const RowID& row_id) {
    const auto& value_id = dictionary_column->attribute_vector()->get(row_id.chunk_offset);
    const auto& value = (*dictionary_column->dictionary()).at(value_id);
    if (matches_scan_type(value, _value_to_find, _type_of_scan)) {
      _result->push_back(row_id);
    }
  }
};

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in), _col_id(column_id), _type_of_scan(scan_type), _value_to_find(search_value) {}

TableScan::~TableScan() {}

ColumnID TableScan::column_id() const { return _col_id; }

ScanType TableScan::scan_type() const { return _type_of_scan; }

const AllTypeVariant& TableScan::search_value() const { return _value_to_find; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  std::shared_ptr<const Table> table_to_scan = _input_table_left();

  const std::string& col_type = table_to_scan->column_type(_col_id);
  auto templated_table_scan =
      make_shared_by_column_type<TableScan::TemplatedTableScanBase, TableScan::TemplatedTableScan>(
          col_type, table_to_scan, _col_id, _type_of_scan, _value_to_find, *this);
  const std::shared_ptr<PosList> table_scan_result = templated_table_scan->_on_execute();

  std::shared_ptr<const Table> table_for_result;
  if (_referenced_table.has_value()) {
    // use referenced table instead to ensure RowIDs, ... are working properly
    table_for_result = _referenced_table.value();
  } else {
    table_for_result = table_to_scan;
  }

  Chunk chunk;
  auto result_table = _create_table_with_schema_of(table_for_result);
  for (ColumnID col_id{0}; col_id < result_table->col_count(); ++col_id) {
    auto reference_column = std::make_shared<ReferenceColumn>(table_for_result, col_id, table_scan_result);
    chunk.add_column(reference_column);
  }
  result_table->emplace_chunk(std::move(chunk));

  return result_table;
}

std::shared_ptr<Table> TableScan::_create_table_with_schema_of(std::shared_ptr<const Table> table) const {
  auto new_table = std::make_shared<Table>(table->chunk_size());
  for (ColumnID col_id{0}; col_id < table->col_count(); ++col_id) {
    const std::string col_type = table->column_type(col_id);
    const std::string col_name = table->column_name(col_id);
    new_table->add_column_definition(col_name, col_type);
  }
  return new_table;
}
}  // namespace opossum
