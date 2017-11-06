#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/types.hpp"
#include "../lib/storage/fitted_attribute_vector.hpp"
  
namespace opossum {

class StorageFittedAttributeVectorTest : public BaseTest {
 protected:
  FittedAttributeVector fav{5};
};

TEST_F(StorageFittedAttributeVectorTest, Width) {
  EXPECT_EQ(fav.width(), sizeof(ValueID)*8);
}

// TODO: more tests

} // namespace opossum