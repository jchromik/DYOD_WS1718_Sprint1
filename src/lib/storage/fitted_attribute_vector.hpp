#pragma once

#include <vector>

#include "types.hpp"
#include "base_attribute_vector.hpp"

namespace opossum {

class FittedAttributeVector : public BaseAttributeVector {
 public:

  explicit FittedAttributeVector(const size_t size) : _entries{size} {}; 

  // returns the value at a given position
  ValueID get(const size_t i) const override;

  // inserts the value_id at a given position
  void set(const size_t i, const ValueID value_id) override;

  // returns the number of values
  size_t size() const override;

  // returns the width of the values in bytes
  AttributeVectorWidth width() const override;

 private:
  std::vector<ValueID> _entries;
};

} // namespace opossum