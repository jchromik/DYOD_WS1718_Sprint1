#include "fitted_attribute_vector.hpp"

#include <limits>

namespace opossum {

template <typename T>
ValueID FittedAttributeVector<T>::get(const size_t i) const {
  if (i >= _entries.size()) {
    throw std::runtime_error("Index out of range");
  }
  return ValueID(_entries.at(i));
}

// this implementation can replace values and append
// if i is not in range [0,_entires.size()] if fails
// also fails if value_is unrepresentably big
template <typename T>
void FittedAttributeVector<T>::set(const size_t i, const ValueID value_id) {
  if (value_id > std::numeric_limits<T>::max()) {
    throw std::runtime_error("ValueID is too big to be represented");
  }

  if (i < _entries.size()) {
    _entries.insert(_entries.cbegin() + i, static_cast<T>(value_id));
  } else if (i == _entries.size()) {
    _entries.push_back(static_cast<T>(value_id));
  } else {
    throw std::runtime_error("Index out of range");
  }
}

template <typename T>
size_t FittedAttributeVector<T>::size() const {
  return _entries.size();
}

// width is returned in bytes, i.e.:
// uint8_t  -> 1
// uint16_t -> 2
// uint32_t -> 4
template <typename T>
AttributeVectorWidth FittedAttributeVector<T>::width() const {
  return AttributeVectorWidth(sizeof(T));
}

template class FittedAttributeVector<uint8_t>;
template class FittedAttributeVector<uint16_t>;
template class FittedAttributeVector<uint32_t>;
// omitting uint64_t because ValueID is only uint32_t

}  // namespace opossum
