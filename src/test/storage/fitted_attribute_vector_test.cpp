#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/types.hpp"
#include "../lib/storage/fitted_attribute_vector.hpp"
  
namespace opossum {

class StorageFittedAttributeVectorTest : public BaseTest {
 protected:
  FittedAttributeVector<uint8_t> fav;
};

TEST_F(StorageFittedAttributeVectorTest, Width) {
  EXPECT_EQ(fav.width(), 1u);
}

TEST_F(StorageFittedAttributeVectorTest, SetGet) {
  fav.set(0, ValueID(2));
  fav.set(1, ValueID(4));
  fav.set(2, ValueID(8));
  fav.set(3, ValueID(16));
  fav.set(4, ValueID(32));
  EXPECT_EQ(fav.get(0), ValueID(2));
  EXPECT_EQ(fav.get(1), ValueID(4));
  EXPECT_EQ(fav.get(2), ValueID(8));
  EXPECT_EQ(fav.get(3), ValueID(16));
  EXPECT_EQ(fav.get(4), ValueID(32));
}

TEST_F(StorageFittedAttributeVectorTest, SetOutOfRange) {
  EXPECT_THROW(fav.set(5, ValueID(2)), std::exception);
}

TEST_F(StorageFittedAttributeVectorTest, GetOutOfRange) {
  EXPECT_THROW(fav.get(5), std::exception);
}

TEST_F(StorageFittedAttributeVectorTest, SetAgain) {
  fav.set(0, ValueID(2));
  fav.set(0, ValueID(4));
  EXPECT_EQ(fav.get(0), ValueID(4));
}

} // namespace opossum