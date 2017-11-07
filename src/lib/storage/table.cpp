#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "dictionary_column.hpp"
#include "value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunk_size(chunk_size) { _chunks.emplace_back(Chunk()); }

void Table::add_column_definition(const std::string& name, const std::string& type) {
  _col_names.push_back(name);
  _col_types.push_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  add_column_definition(name, type);
  for (auto& chunk : _chunks) {
    chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_chunks.back().size() == _chunk_size && _chunk_size != 0) {
    create_new_chunk();
  }
  _chunks.back().append(values);
}

void Table::create_new_chunk() {
  _chunks.push_back(Chunk());
  auto& new_chunk = _chunks.back();
  for (const auto& type : _col_types) {
    new_chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }
}

uint16_t Table::col_count() const { return _col_names.size(); }

uint64_t Table::row_count() const {
  uint64_t row_count = 0;
  for (auto& chunk : _chunks) {
    row_count += chunk.size();
  }
  return row_count;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  for (uint16_t col_index = 0; col_index < col_count(); ++col_index) {
    if (column_name == _col_names.at(col_index)) {
      return ColumnID{col_index};
    }
  }
  throw std::runtime_error("Column not found");
}

uint32_t Table::chunk_size() const { return _chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _col_names; }

const std::string& Table::column_name(ColumnID column_id) const {
  if (_col_names.size() <= static_cast<size_t>(column_id)) {
    throw std::runtime_error("Column not found");
  }
  return _col_names.at(column_id);
}

const std::string& Table::column_type(ColumnID column_id) const {
  if (_col_types.size() <= static_cast<size_t>(column_id)) {
    throw std::runtime_error("Column not found");
  }
  return _col_types.at(column_id);
}

Chunk& Table::get_chunk(ChunkID chunk_id) { return _chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return _chunks.at(chunk_id); }

void Table::compress_chunk(ChunkID chunk_id) {
  Chunk compressed_chunk = Chunk();
  Chunk& raw_chunk = _chunks.at(chunk_id);

  for (auto i = 0; i < raw_chunk.col_count(); ++i) {
    compressed_chunk.add_column(
        make_shared_by_column_type<BaseColumn, DictionaryColumn>(_col_types.at(i), raw_chunk.get_column(ColumnID(i))));
  }

  _chunks.erase(_chunks.begin() + chunk_id);
  _chunks.emplace(_chunks.begin() + chunk_id, std::move(compressed_chunk));
}

}  // namespace opossum
