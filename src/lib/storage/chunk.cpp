#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_column(std::shared_ptr<BaseColumn> column) {
  _columns.push_back(column);
}

void Chunk::append(std::vector<AllTypeVariant> values) {
  DebugAssert(values.size() == _columns.size(), "values do not match columns");

  auto val_it = values.begin();
  auto col_it = _columns.begin();
  while(val_it != values.end() && col_it !=_columns.end()) {
    col_it->get()->append(*val_it);
    ++val_it;
    ++col_it;
  }
}

std::shared_ptr<BaseColumn> Chunk::get_column(ColumnID column_id) const {
  return _columns.at(column_id);
}

uint16_t Chunk::col_count() const {
  return _columns.size();
}

uint32_t Chunk::size() const {
  if (_columns.size() == 0) {
    return 0;
  }
  return _columns.at(0)->size();
}

}  // namespace opossum
