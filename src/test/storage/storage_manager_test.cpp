#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageStorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = StorageManager::get();
    auto t1 = std::make_shared<Table>();
    auto t2 = std::make_shared<Table>(4);

    sm.add_table("first_table", t1);
    sm.add_table("second_table", t2);
  }
};

TEST_F(StorageStorageManagerTest, GetTable) {
  auto& sm = StorageManager::get();
  auto t3 = sm.get_table("first_table");
  auto t4 = sm.get_table("second_table");
  EXPECT_THROW(sm.get_table("third_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DropTable) {
  auto& sm = StorageManager::get();
  sm.drop_table("first_table");
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
  EXPECT_THROW(sm.drop_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, ResetTable) {
  StorageManager::reset();
  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DoesNotHaveTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("third_table"), false);
}

TEST_F(StorageStorageManagerTest, HasTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("first_table"), true);
}

TEST_F(StorageStorageManagerTest, TableNames) {
  auto& sm = StorageManager::get();
  std::vector<std::string> expected;
  expected.push_back("first_table");
  expected.push_back("second_table");
  EXPECT_EQ(sm.table_names(), expected);
}

TEST_F(StorageStorageManagerTest, Print) {
  auto& sm = StorageManager::get();
  std::ostringstream test_stream;
  sm.print(test_stream);
  std::string expected = "Table Name (#Columns, #Rows, #Chunks)\nfirst_table (0, 0, 1)\nsecond_table (0, 0, 1)\n";
  EXPECT_STREQ(test_stream.str().c_str(), expected.c_str());
}

}  // namespace opossum
