// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <sstream>

#include <arrow/pretty_print.h>
#include <gtest/gtest.h>
#include <cmath>
#include "arrow/memory_pool.h"
#include "arrow/status.h"

#include <iostream>
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::float64;
using arrow::int32;
using arrow::int64;
using arrow::utf8;

class TestHash : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestHash, TestSimple) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto field_b = field("b", int32());
  auto field_c = field("c", int64());
  auto field_d = field("d", arrow::float32());
  auto field_e = field("e", arrow::float64());
  auto field_f = field("f", arrow::boolean());
  auto schema = arrow::schema({field_a, field_b, field_c, field_d, field_e, field_f});

  // output fields
  auto res_0 = field("res0", int32());
  auto res_1 = field("res1", int64());
  auto res_2 = field("res2", int32());
  auto res_3 = field("res3", int32());
  auto res_4 = field("res4", int32());
  auto res_5 = field("res5", int32());
  auto res_6 = field("res6", int32());

  // build expression.
  // hash32(a, 10)
  // hash64(a)
  float java_fNaN;
  double java_dNaN;
  int32_t java_fNaN_raw = 2143289344;
  uint64_t java_dNaN_raw = 0x7ff8000000000000;
  memcpy(&java_fNaN, &java_fNaN_raw, 4);
  memcpy(&java_dNaN, &java_dNaN_raw, 8);
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto node_d = TreeExprBuilder::MakeField(field_d);
  auto node_e = TreeExprBuilder::MakeField(field_e);
  auto node_f = TreeExprBuilder::MakeField(field_f);
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto literal_0 = TreeExprBuilder::MakeLiteral((int32_t)0);
  auto literal_dn0 = TreeExprBuilder::MakeLiteral((double)-0.0);
  auto literal_d0 = TreeExprBuilder::MakeLiteral((double)0.0);
  auto literal_Java_fNaN = TreeExprBuilder::MakeLiteral(java_fNaN);
  auto literal_Java_dNaN = TreeExprBuilder::MakeLiteral(java_dNaN);
  auto isnan_f = TreeExprBuilder::MakeFunction("isNaN", {node_d}, boolean());
  auto isnan_d = TreeExprBuilder::MakeFunction("isNaN", {node_e}, boolean());
  auto iszero_d =
      TreeExprBuilder::MakeFunction("equal", {node_e, literal_dn0}, boolean());
  auto process_nan_f =
      TreeExprBuilder::MakeIf(isnan_f, literal_Java_fNaN, node_d, float32());
  auto process_zero_d = TreeExprBuilder::MakeIf(iszero_d, literal_d0, node_e, float64());
  auto process_nan_d =
      TreeExprBuilder::MakeIf(isnan_d, literal_Java_dNaN, process_zero_d, float64());

  auto hash32 = TreeExprBuilder::MakeFunction("hash32", {node_a, literal_10}, int32());
  auto hash32_spark =
      TreeExprBuilder::MakeFunction("hash32_spark", {node_b, literal_0}, int32());
  auto hash64 = TreeExprBuilder::MakeFunction("hash64", {node_a}, int64());
  auto hash64_spark =
      TreeExprBuilder::MakeFunction("hash64_spark", {node_c, literal_0}, int32());
  auto hash32_spark_float =
      TreeExprBuilder::MakeFunction("hash32_spark", {process_nan_f, literal_0}, int32());
  auto hash64_spark_double =
      TreeExprBuilder::MakeFunction("hash64_spark", {process_nan_d, literal_0}, int32());
  auto hash32_spark_bool =
      TreeExprBuilder::MakeFunction("hash32_spark", {node_f, literal_0}, int32());
  auto expr_0 = TreeExprBuilder::MakeExpression(hash32, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(hash64, res_1);
  auto expr_2 = TreeExprBuilder::MakeExpression(hash32_spark, res_2);
  auto expr_3 = TreeExprBuilder::MakeExpression(hash64_spark, res_3);
  auto expr_4 = TreeExprBuilder::MakeExpression(hash32_spark_float, res_4);
  auto expr_5 = TreeExprBuilder::MakeExpression(hash64_spark_double, res_5);
  auto expr_6 = TreeExprBuilder::MakeExpression(hash32_spark_bool, res_6);

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_0, expr_1, expr_2, expr_3, expr_4, expr_5, expr_6},
                      TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayInt32({1, 2, 3, 4}, {false, true, true, true});
  auto array_b = MakeArrowArrayInt32({0, -42, 42, 2143289344}, {true, true, true, true});
  auto array_c = MakeArrowArrayInt64({0L, -42L, 42L, 9221120237041090560L},
                                     {true, true, true, true});
  auto array_d = MakeArrowArrayFloat32({706.17, INFINITY, (float)2143289344, NAN},
                                       {true, true, false, true});
  auto array_e = MakeArrowArrayFloat64(
      {706.17, INFINITY, (double)9221120237041090560, -0.0}, {true, true, false, true});
  auto array_f = MakeArrowArrayBool({1, 1, 0, 0}, {false, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(
      schema, num_records, {array_a, array_b, array_c, array_d, array_e, array_f});

  // arrow::PrettyPrint(*in_batch.get(), 2, &std::cout);
  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  EXPECT_EQ(int32_arr->null_count(), 0);
  EXPECT_EQ(int32_arr->Value(0), 10);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int32_arr->Value(i), int32_arr->Value(i - 1));
  }

  auto int64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(1));
  EXPECT_EQ(int64_arr->null_count(), 0);
  EXPECT_EQ(int64_arr->Value(0), 0);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int64_arr->Value(i), int64_arr->Value(i - 1));
  }

  auto int32_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(2));
  std::vector<int32_t> int32_spark_arr_expect = {593689054, -189366624, -1134849565,
                                                 1927335251};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(int32_spark_arr->Value(i), int32_spark_arr_expect[i]);
  }

  auto int64_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(3));
  std::vector<int32_t> int64_spark_arr_expect = {1669671676, -846261623, 1871679806,
                                                 1428788237};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(int64_spark_arr->Value(i), int64_spark_arr_expect[i]);
  }

  auto float32_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(4));
  std::vector<int32_t> float32_spark_arr_expect = {871666867, 1927335251, 0, 1927335251};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(float32_spark_arr->Value(i), float32_spark_arr_expect[i]);
  }

  auto float64_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(5));
  std::vector<int32_t> float64_spark_arr_expect = {1942731644, 1428788237, 0, 1669671676};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(float64_spark_arr->Value(i), float64_spark_arr_expect[i]);
  }

  auto bool_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(6));
  std::vector<int32_t> bool_spark_arr_expect = {0, -68075478, 593689054, 593689054};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(bool_spark_arr->Value(i), bool_spark_arr_expect[i]);
  }
}

TEST_F(TestHash, TestBuf) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto field_b = field("b", utf8());
  auto schema = arrow::schema({field_a, field_b});

  // output fields
  auto res_0 = field("res0", int32());
  auto res_1 = field("res1", int64());
  auto res_2 = field("res2", int32());

  // build expressions.
  // hash32(a)
  // hash64(a, 10)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto literal_10 = TreeExprBuilder::MakeLiteral((int64_t)10);
  auto literal_0 = TreeExprBuilder::MakeLiteral((int32_t)0);
  auto hash32 = TreeExprBuilder::MakeFunction("hash32", {node_a}, int32());
  auto hash64 = TreeExprBuilder::MakeFunction("hash64", {node_a, literal_10}, int64());
  auto hash32_spark =
      TreeExprBuilder::MakeFunction("hashbuf_spark", {node_b, literal_0}, int32());
  auto expr_0 = TreeExprBuilder::MakeExpression(hash32, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(hash64, res_1);
  auto expr_2 = TreeExprBuilder::MakeExpression(hash32_spark, res_2);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_0, expr_1, expr_2}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"foo", "hello", "bye", "hi"}, {false, true, true, true});
  auto array_b =
      MakeArrowArrayUtf8({"test", "test1", "te", "tes"}, {true, true, true, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  EXPECT_EQ(int32_arr->null_count(), 0);
  EXPECT_EQ(int32_arr->Value(0), 0);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int32_arr->Value(i), int32_arr->Value(i - 1));
  }

  auto int64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(1));
  EXPECT_EQ(int64_arr->null_count(), 0);
  EXPECT_EQ(int64_arr->Value(0), 10);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int64_arr->Value(i), int64_arr->Value(i - 1));
  }

  auto utf8_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(2));
  std::vector<int32_t> utf8_spark_arr_expect = {-1167338989, -1136150618, -2074114216, 0};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(utf8_spark_arr->Value(i), utf8_spark_arr_expect[i]);
  }
}

TEST_F(TestHash, TestBuf2) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto field_b = field("b", utf8());
  auto schema = arrow::schema({field_a, field_b});

  // output fields
  auto res_0 = field("res0", int32());
  auto res_1 = field("res1", int32());

  // build expressions.
  // hash32(a)
  // hash64(a, 10)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto literal_42 = TreeExprBuilder::MakeLiteral((int32_t)42);
  auto node_seed =
      TreeExprBuilder::MakeFunction("hashbuf_spark", {node_a, literal_42}, int32());
  auto hash32_spark =
      TreeExprBuilder::MakeFunction("hashbuf_spark", {node_b, node_seed}, int32());
  auto expr_0 = TreeExprBuilder::MakeExpression(hash32_spark, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(node_seed, res_1);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_0, expr_1}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"Williams", "Smith", "", ""}, {true, true, false, false});
  auto array_b =
      MakeArrowArrayUtf8({"Doug", "Kathleen", "", ""}, {true, true, false, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto utf8_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  std::vector<int32_t> utf8_spark_arr_expect = {1506520301, 648517158, 42, 42};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(utf8_spark_arr->Value(i), utf8_spark_arr_expect[i]);
  }
  auto seed_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(1));
  std::vector<int32_t> seed_arr_expect = {-1535375369, 1628584033, 42, 42};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(seed_arr->Value(i), seed_arr_expect[i]);
  }
}

}  // namespace gandiva
