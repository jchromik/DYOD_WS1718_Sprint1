#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../utils/assert.hpp"
#include "all_type_variant.hpp"
#include "base_attribute_vector.hpp"
#include "fitted_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "value_column.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseColumn;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific column type that stores all its values in a vector
template <typename T>
class DictionaryColumn : public BaseColumn {
 public:
  /**
   * Creates a Dictionary column from a given value column.
   */
  explicit DictionaryColumn(const std::shared_ptr<BaseColumn>& base_column) {
    // Doing this since dictionary compression only works for ValueColumns (for now).
    const std::shared_ptr<ValueColumn<T>>& value_column_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(base_column);
    const ValueColumn<T>& value_column = *value_column_ptr;

    _fill_dictionary(value_column);

    _create_attribute_vector(_dictionary->size());

    _fill_attribute_vector(value_column);
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionaryColumn in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override {
    return AllTypeVariant(_dictionary->at(_attribute_vector->get(i)));
  }

  // return the value at a certain position.
  const T get(const size_t i) const { return _dictionary->at(_attribute_vector->get(i)); }

  // dictionary columns are immutable
  void append(const AllTypeVariant&) override { throw std::runtime_error("Can not append to dictionary column"); }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    auto lower = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (lower != _dictionary->cend()) {
      return ValueID(std::distance(_dictionary->cbegin(), lower));
    }
    return INVALID_VALUE_ID;
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(type_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    auto upper = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (upper != _dictionary->cend()) {
      return ValueID(std::distance(_dictionary->cbegin(), upper));
    }
    return INVALID_VALUE_ID;
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(type_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

//  void process_column_in_table_scan(const TableScan& table_scan) override {
//    table_scan->process_dictionary_column(*this);
//  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;

  // Build up dictionary
  void _fill_dictionary(const ValueColumn<T>& value_column) {
    std::set<T> dict;
    for (size_t index = 0; index < value_column.size(); ++index) {
      dict.insert(type_cast<T>(value_column[index]));
    }
    _dictionary = std::make_shared<std::vector<T>>(dict.cbegin(), dict.cend());
  }

  // Create attribute vector capable of taking num_values entries
  void _create_attribute_vector(const size_t num_values) {
    if (num_values <= std::numeric_limits<uint8_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint8_t>>();
    } else if (num_values <= std::numeric_limits<uint16_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint16_t>>();
    } else {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint32_t>>();
    }
  }

  // Fill attribute vector using values from value_column and value ids from dictionary
  void _fill_attribute_vector(const ValueColumn<T>& value_column) {
    for (size_t index = 0; index < value_column.size(); ++index) {
      const auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), type_cast<T>(value_column[index]));
      Assert(it != _dictionary->cend(), "Dictionary creation or lookup failed");
      const ValueID value_id = ValueID(it - _dictionary->cbegin());
      _attribute_vector->set(index, value_id);
    }
  }
};

}  // namespace opossum
