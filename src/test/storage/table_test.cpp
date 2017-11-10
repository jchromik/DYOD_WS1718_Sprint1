#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  Table t{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  t.get_chunk(ChunkID{0});
  EXPECT_THROW(t.get_chunk(ChunkID{1}), std::exception);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.get_chunk(ChunkID{1});
  const auto& chunk = static_cast<const Table&>(t).get_chunk(ChunkID{1});
  EXPECT_EQ(chunk.size(), 1u);
}

TEST_F(StorageTableTest, ColCount) { EXPECT_EQ(t.col_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.chunk_size(), 2u); }

TEST_F(StorageTableTest, CompressChunk) {
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});

  t.compress_chunk(ChunkID{0});
  EXPECT_EQ(t.chunk_count(), 2u);

  t.compress_chunk(ChunkID{1});
  EXPECT_EQ(t.chunk_count(), 2u);

  EXPECT_EQ(t.row_count(), 3u);
  EXPECT_EQ(t.col_count(), 2u);

  EXPECT_THROW(t.append({5, "Foo"}), std::exception);
}

TEST_F(StorageTableTest, AddColumnToChunkAlreadyContainingValues) {
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.add_column("col_3", "int");
  t.append({2, "NewEntry", 3});
  Chunk& c1 = t.get_chunk(ChunkID{0});
  Chunk& c2 = t.get_chunk(ChunkID{1});
  // all columns should have same number of rows
  EXPECT_EQ(2u, c1.get_column(ColumnID{0})->size());
  EXPECT_EQ(2u, c1.get_column(ColumnID{1})->size());
  EXPECT_EQ(2u, c1.get_column(ColumnID{2})->size());
  EXPECT_EQ(2u, c2.get_column(ColumnID{0})->size());
  EXPECT_EQ(2u, c2.get_column(ColumnID{1})->size());
  EXPECT_EQ(2u, c2.get_column(ColumnID{2})->size());
}

TEST_F(StorageTableTest, ColumnNames) {
  EXPECT_EQ(t.column_names().size(), 2u);
  EXPECT_EQ(t.column_names().at(0), "col_1");
  EXPECT_EQ(t.column_names().at(1), "col_2");
}

}  // namespace opossum
