#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

void Table::add_column_definition(const std::string& name, const std::string& type) {
  _colnames.push_back(name);
  _coltypes.push_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  add_column_definition(name, type);
  for (auto & chunk : _chunks) {
    chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_chunks.size() == 0) {
    create_new_chunk();
  }
  if (_chunks.back().size() >= _chunk_size && _chunk_size != 0) {
    create_new_chunk();
  }
  _chunks.back().append(values);
}

void Table::create_new_chunk() {
  _chunks.push_back(Chunk());
  for (auto type : _coltypes) {
    _chunks.back().add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }
}

uint16_t Table::col_count() const {
  return static_cast<uint16_t>(_colnames.size());
}

uint64_t Table::row_count() const {
  uint64_t row_count = 0;
  for(auto & chunk : _chunks) {
    row_count += chunk.size();
  }
  return row_count;
}

ChunkID Table::chunk_count() const {
  return ChunkID{static_cast<ChunkID>(_chunks.size())};
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  for (size_t i = 0; i < _colnames.size(); ++i) {
    if (column_name == _colnames.at(i)) {
      return ColumnID{static_cast<ColumnID>(i)};
    }
  }
  throw std::runtime_error("Column not found");
}

uint32_t Table::chunk_size() const {
  return _chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  return _colnames;
}

const std::string& Table::column_name(ColumnID column_id) const {
  if (_colnames.size() <= static_cast<size_t>(column_id)) {
    throw std::runtime_error("Column not found");
  }
  return _colnames.at(column_id);
}

const std::string& Table::column_type(ColumnID column_id) const {
  if (_coltypes.size() <= static_cast<size_t>(column_id)) {
    throw std::runtime_error("Column not found");
  }
  return _coltypes.at(column_id);
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  if (_chunks.size() <= static_cast<size_t>(chunk_id)) {
    throw std::runtime_error("Chunk does not exist");
  }
  return _chunks.at(chunk_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  return const_cast<Table*>( this )->get_chunk(chunk_id);
}

}  // namespace opossum
