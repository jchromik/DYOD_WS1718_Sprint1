#include "fitted_attribute_vector.hpp"

namespace opossum {

template <typename T>
ValueID FittedAttributeVector<T>::get(const size_t i) const {
  // TODO
  return ValueID{0};
}

template <typename T>
void FittedAttributeVector<T>::set(const size_t i, const ValueID value_id) {
  // TODO
  return;
}

template <typename T>
size_t FittedAttributeVector<T>::size() const {
  // TODO
  return 0;
}

template <typename T>
AttributeVectorWidth FittedAttributeVector<T>::width() const {
  // TODO
  return AttributeVectorWidth{0};
}

} // namespace opossum