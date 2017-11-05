#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <set>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "type_cast.hpp"
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
    // Using dynamic_pointer_cast to go down/across class hierarchy.
    // (cf: http://en.cppreference.com/w/cpp/memory/shared_ptr/pointer_cast)
    // Doing this since dictionary compression only works for ValueColumns (for now).
    const std::shared_ptr<ValueColumn<T>>& value_column_ptr = std::dynamic_pointer_cast<ValueColumn<T>>(base_column);
    const ValueColumn<T>& value_column = *value_column_ptr;
    
    // Build up dictionary.
    // In this implementation we do not rely on the property of std::sets being sorted,
    // since this is an implementation detail not part of the specification.
    std::set<T> dict;
    for(size_t i = 0; i < value_column.size(); ++i) {
      dict.insert(type_cast<T>(value_column[i]));
    }
    _dictionary = std::make_shared<std::vector<T>>(dict.begin(), dict.end());
    std::sort(_dictionary->begin(), _dictionary->end());

    // Fill attribute vector.
    _attribute_vector = std::make_shared<std::vector<ValueID>>();
    for(size_t i = 0; i < value_column.size(); ++i) {
      auto it = std::find(_dictionary->begin(), _dictionary->end(), type_cast<T>(value_column[i]));
      Assert(it != _dictionary->end(), "Dictionary creation or lookup failed");
      ValueID value_id = ValueID(it - _dictionary->begin());
      _attribute_vector->emplace_back(value_id); 
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionaryColumn in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override {
    return AllTypeVariant(_dictionary->at(_attribute_vector->at(i)));
  }

  // return the value at a certain position.
  const T get(const size_t i) const {
    return _dictionary->at(_attribute_vector->at(i));
  }

  // dictionary columns are immutable
  void append(const AllTypeVariant&) override {
    // TODO: Better do nothing instead of throwing?
    throw std::runtime_error("Can not append to dictionary column");
  }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const {
    return _dictionary;
  }

  // returns an underlying data structure
  // std::shared_ptr<const BaseAttributeVector> attribute_vector() const;
  // FOR NOW (=TODO):
  std::shared_ptr<const std::vector<uint64_t>> attribute_vector() const {
    return _attribute_vector;
  }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const {
    return _dictionary->at(value_id);
  }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    for(size_t i = 0; i < _dictionary->size(); ++i) {
      if(_dictionary->at(i) >= value) {
        return static_cast<ValueID>(i);
      }
    }
    return INVALID_VALUE_ID;
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const {
    // TODO: Check if dynamic_cast is really the correct choice here.
    return lower_bound(dynamic_cast<T>(value));
  }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    for(size_t i = 0; i < _dictionary->size(); ++i) {
      if(_dictionary->at(i) > value) {
        return static_cast<ValueID>(i);
      }
    }
    return INVALID_VALUE_ID;
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const {
    // TODO: Check if dynamic_cast is really the correct choice here.
    return upper_bound(dynamic_cast<T>(value));
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const {
    return _dictionary->size();
  }

  // return the number of entries
  size_t size() const override {
    return _attribute_vector->size();
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  // std::shared_ptr<BaseAttributeVector> _attribute_vector;
  // FOR NOW (=TODO):
  std::shared_ptr<std::vector<ValueID>> _attribute_vector;
};

}  // namespace opossum
