#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <set>

#include "all_type_variant.hpp"
#include "types.hpp"

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
    // build up dictionary
    std::set<T> dict;
    for(size_t i = 0; i < base_column->size(); ++i) {
      dict.insert(base_column.get()[i]);
    }
    _dictionary = std::vector(dict.begin(), dict.end());
    std::sort(_dictionary.begin(), _dictionary.end());

    // fill attribute vector
    for(size_t i = 0; i < base_column->size(); ++i) {
      std::find(_dictionary.begin(), _dictionary.end(), base_column.get()[i]);
      // TODO
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionaryColumn in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override {
    // TODO: Do we really need this dynamic cast?
    return dynamic_cast<AllTypeVariant>(_dictionary->at(_attribute_vector->at(i)));
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
        return i;
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
        return i;
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
  std::shared_ptr<std::vector<uint64_t>> _attribute_vector;
};

}  // namespace opossum
