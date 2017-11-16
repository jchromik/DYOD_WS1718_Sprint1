#include "reference_column.hpp"

#include <memory>

namespace opossum {

ReferenceColumn::ReferenceColumn(const std::shared_ptr<const Table> referenced_table,
                                 const ColumnID referenced_column_id,
                                 const std::shared_ptr<const PosList> pos) 
  : _table(referenced_table), _column_id(referenced_column_id), _positions(pos) { }

const AllTypeVariant ReferenceColumn::operator[](const size_t i) const {
  if(i > _table->row_count()) {
    throw std::runtime_error("Table index out of bounds");
  }
  // find corresponding chunk
  const RowID& row_id = _positions->at(i);
  const Chunk& chunk = _table->get_chunk(row_id.chunk_id);
  const auto& column = chunk.get_column(_column_id);
  return (*column)[row_id.chunk_offset];
}

size_t ReferenceColumn::size() const {
  return _positions->size();
}

const std::shared_ptr<const PosList> ReferenceColumn::pos_list() const {
  return _positions;
}

const std::shared_ptr<const Table> ReferenceColumn::referenced_table() const {
  return _table;
}

ColumnID ReferenceColumn::referenced_column_id() const {
  return _column_id;
}

} // namespace opossum