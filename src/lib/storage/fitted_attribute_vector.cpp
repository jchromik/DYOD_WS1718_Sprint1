#include "fitted_attribute_vector.hpp"

namespace opossum {

ValueID FittedAttributeVector::get(const size_t i) const {
  return ValueID{_entries.at(i)};
}

void FittedAttributeVector::set(const size_t i, const ValueID value_id) {
  _entries.erase(_entries.begin() + i);
  _entries.emplace(_entries.begin() + i, value_id);
}

size_t FittedAttributeVector::size() const {
  return _entries.size();
}

AttributeVectorWidth FittedAttributeVector::width() const {
  // TODO: Is this too hacky?
  return AttributeVectorWidth{8 * sizeof(ValueID)};
}

} // namespace opossum