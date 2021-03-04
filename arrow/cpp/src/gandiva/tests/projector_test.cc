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

#include "gandiva/projector.h"

#include <arrow/type.h>
#include <gtest/gtest.h>

#include <cmath>

#include "arrow/memory_pool.h"
#include "gandiva/literal_holder.h"
#include "gandiva/node.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class TestProjector : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestProjector, TestProjectCache) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr =
      TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_sub);

  auto configuration = TestConfiguration();

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {sum_expr, sub_expr}, configuration, &projector);
  ASSERT_OK(status);

  // everything is same, should return the same projector.
  auto schema_same = arrow::schema({field0, field1});
  std::shared_ptr<Projector> cached_projector;
  status = Projector::Make(schema_same, {sum_expr, sub_expr}, configuration,
                           &cached_projector);
  ASSERT_OK(status);
  EXPECT_EQ(cached_projector, projector);

  // schema is different should return a new projector.
  auto field2 = field("f2", int32());
  auto different_schema = arrow::schema({field0, field1, field2});
  std::shared_ptr<Projector> should_be_new_projector;
  status = Projector::Make(different_schema, {sum_expr, sub_expr}, configuration,
                           &should_be_new_projector);
  ASSERT_OK(status);
  EXPECT_NE(cached_projector, should_be_new_projector);

  // expression list is different should return a new projector.
  std::shared_ptr<Projector> should_be_new_projector1;
  status = Projector::Make(schema, {sum_expr}, configuration, &should_be_new_projector1);
  ASSERT_OK(status);
  EXPECT_NE(cached_projector, should_be_new_projector1);

  // another instance of the same configuration, should return the same projector.
  status = Projector::Make(schema, {sum_expr, sub_expr}, TestConfiguration(),
                           &cached_projector);
  ASSERT_OK(status);
  EXPECT_EQ(cached_projector, projector);
}

TEST_F(TestProjector, TestProjectCacheFieldNames) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto field2 = field("f2", int32());
  auto schema = arrow::schema({field0, field1, field2});

  // output fields
  auto sum_01 = field("sum_01", int32());
  auto sum_12 = field("sum_12", int32());

  auto sum_expr_01 = TreeExprBuilder::MakeExpression("add", {field0, field1}, sum_01);
  std::shared_ptr<Projector> projector_01;
  auto status =
      Projector::Make(schema, {sum_expr_01}, TestConfiguration(), &projector_01);
  EXPECT_TRUE(status.ok());

  auto sum_expr_12 = TreeExprBuilder::MakeExpression("add", {field1, field2}, sum_12);
  std::shared_ptr<Projector> projector_12;
  status = Projector::Make(schema, {sum_expr_12}, TestConfiguration(), &projector_12);
  EXPECT_TRUE(status.ok());

  // add(f0, f1) != add(f1, f2)
  EXPECT_TRUE(projector_01.get() != projector_12.get());
}

TEST_F(TestProjector, TestProjectCacheDouble) {
  auto schema = arrow::schema({});
  auto res = field("result", arrow::float64());

  double d0 = 1.23456788912345677E18;
  double d1 = 1.23456789012345677E18;

  auto literal0 = TreeExprBuilder::MakeLiteral(d0);
  auto expr0 = TreeExprBuilder::MakeExpression(literal0, res);
  auto configuration = TestConfiguration();

  std::shared_ptr<Projector> projector0;
  auto status = Projector::Make(schema, {expr0}, configuration, &projector0);
  EXPECT_TRUE(status.ok()) << status.message();

  auto literal1 = TreeExprBuilder::MakeLiteral(d1);
  auto expr1 = TreeExprBuilder::MakeExpression(literal1, res);
  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr1}, configuration, &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector0.get() != projector1.get());
}

TEST_F(TestProjector, TestProjectCacheFloat) {
  auto schema = arrow::schema({});
  auto res = field("result", arrow::float32());

  float f0 = static_cast<float>(12345678891.000000);
  float f1 = f0 - 1000;

  auto literal0 = TreeExprBuilder::MakeLiteral(f0);
  auto expr0 = TreeExprBuilder::MakeExpression(literal0, res);
  std::shared_ptr<Projector> projector0;
  auto status = Projector::Make(schema, {expr0}, TestConfiguration(), &projector0);
  EXPECT_TRUE(status.ok()) << status.message();

  auto literal1 = TreeExprBuilder::MakeLiteral(f1);
  auto expr1 = TreeExprBuilder::MakeExpression(literal1, res);
  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector0.get() != projector1.get());
}

TEST_F(TestProjector, TestProjectCacheLiteral) {
  auto schema = arrow::schema({});
  auto res = field("result", arrow::decimal(38, 5));

  DecimalScalar128 d0("12345678", 38, 5);
  DecimalScalar128 d1("98756432", 38, 5);

  auto literal0 = TreeExprBuilder::MakeDecimalLiteral(d0);
  auto expr0 = TreeExprBuilder::MakeExpression(literal0, res);
  std::shared_ptr<Projector> projector0;
  ASSERT_OK(Projector::Make(schema, {expr0}, TestConfiguration(), &projector0));

  auto literal1 = TreeExprBuilder::MakeDecimalLiteral(d1);
  auto expr1 = TreeExprBuilder::MakeExpression(literal1, res);
  std::shared_ptr<Projector> projector1;
  ASSERT_OK(Projector::Make(schema, {expr1}, TestConfiguration(), &projector1));

  EXPECT_NE(projector0.get(), projector1.get());
}

TEST_F(TestProjector, TestProjectCacheDecimalCast) {
  auto field_float64 = field("float64", arrow::float64());
  auto schema = arrow::schema({field_float64});

  auto res_31_13 = field("result", arrow::decimal(31, 13));
  auto expr0 = TreeExprBuilder::MakeExpression("castDECIMAL", {field_float64}, res_31_13);
  std::shared_ptr<Projector> projector0;
  ASSERT_OK(Projector::Make(schema, {expr0}, TestConfiguration(), &projector0));

  // if the output scale is different, the cache can't be used.
  auto res_31_14 = field("result", arrow::decimal(31, 14));
  auto expr1 = TreeExprBuilder::MakeExpression("castDECIMAL", {field_float64}, res_31_14);
  std::shared_ptr<Projector> projector1;
  ASSERT_OK(Projector::Make(schema, {expr1}, TestConfiguration(), &projector1));
  EXPECT_NE(projector0.get(), projector1.get());

  // if the output scale/precision are same, should get a cache hit.
  auto res_31_13_alt = field("result", arrow::decimal(31, 13));
  auto expr2 =
      TreeExprBuilder::MakeExpression("castDECIMAL", {field_float64}, res_31_13_alt);
  std::shared_ptr<Projector> projector2;
  ASSERT_OK(Projector::Make(schema, {expr2}, TestConfiguration(), &projector2));
  EXPECT_EQ(projector0.get(), projector2.get());
}

TEST_F(TestProjector, TestShiftRight) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_shift_right = field("shift_right", int32());

  // Build expression
  auto shift_right_expr = TreeExprBuilder::MakeExpression("shift_right", {field0, field1},
      field_shift_right);

  std::shared_ptr<Projector> projector;

  auto status =
      Projector::Make(schema, {shift_right_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  if (!status.ok()) {
    std::cout << status.message() << std::endl;
  }

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({4, 8, 16, 32}, {true, true, true, true});
  auto array1 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, true});

  // expected output
  auto exp_shift_right = MakeArrowArrayInt32({2, 2, 2, 2}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_shift_right, outputs.at(0));
}


TEST_F(TestProjector, TestShiftLeft) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_shift_left = field("shift_left", int32());

  // Build expression
  auto shift_left_expr = TreeExprBuilder::MakeExpression("shift_left", {field0, field1},
                                                          field_shift_left);

  std::shared_ptr<Projector> projector;

  auto status =
      Projector::Make(schema, {shift_left_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  if (!status.ok()) {
    std::cout << status.message() << std::endl;
  }

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({4, 8, 16, 32}, {true, true, true, true});
  auto array1 = MakeArrowArrayInt32({4, 3, 2, 1}, {true, true, true, true});

  // expected output
  auto exp_shift_left = MakeArrowArrayInt32({64, 64, 64, 64}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_shift_left, outputs.at(0));
}

TEST_F(TestProjector, TestIntSumSub) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr =
      TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_sub);

  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {sum_expr, sub_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, false});
  auto array1 = MakeArrowArrayInt32({11, 13, 15, 17}, {true, true, false, true});
  // expected output
  auto exp_sum = MakeArrowArrayInt32({12, 15, 0, 0}, {true, true, false, false});
  auto exp_sub = MakeArrowArrayInt32({-10, -11, 0, 0}, {true, true, false, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_sub, outputs.at(1));
}

template <typename TYPE, typename C_TYPE>
static void TestArithmeticOpsForType(arrow::MemoryPool* pool) {
  auto atype = arrow::TypeTraits<TYPE>::type_singleton();

  // schema for input fields
  auto field0 = field("f0", atype);
  auto field1 = field("f1", atype);
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_sum = field("add", atype);
  auto field_sub = field("subtract", atype);
  auto field_mul = field("multiply", atype);
  auto field_div = field("divide", atype);
  auto field_eq = field("equal", arrow::boolean());
  auto field_lt = field("less_than", arrow::boolean());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr =
      TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_sub);
  auto mul_expr =
      TreeExprBuilder::MakeExpression("multiply", {field0, field1}, field_mul);
  auto div_expr = TreeExprBuilder::MakeExpression("divide", {field0, field1}, field_div);
  auto eq_expr = TreeExprBuilder::MakeExpression("equal", {field0, field1}, field_eq);
  auto lt_expr = TreeExprBuilder::MakeExpression("less_than", {field0, field1}, field_lt);

  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {sum_expr, sub_expr, mul_expr, div_expr, eq_expr, lt_expr},
                      TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 12;
  std::vector<C_TYPE> input0 = {1, 2, 53, 84, 5, 15, 0, 1, 52, 83, 4, 120};
  std::vector<C_TYPE> input1 = {10, 15, 23, 84, 4, 51, 68, 9, 16, 18, 19, 37};
  std::vector<bool> validity = {true, true, true, true, true, true,
                                true, true, true, true, true, true};

  auto array0 = MakeArrowArray<TYPE, C_TYPE>(input0, validity);
  auto array1 = MakeArrowArray<TYPE, C_TYPE>(input1, validity);

  // expected output
  std::vector<C_TYPE> sum;
  std::vector<C_TYPE> sub;
  std::vector<C_TYPE> mul;
  std::vector<C_TYPE> div;
  std::vector<bool> eq;
  std::vector<bool> lt;
  for (int i = 0; i < num_records; i++) {
    sum.push_back(static_cast<C_TYPE>(input0[i] + input1[i]));
    sub.push_back(static_cast<C_TYPE>(input0[i] - input1[i]));
    mul.push_back(static_cast<C_TYPE>(input0[i] * input1[i]));
    div.push_back(static_cast<C_TYPE>(input0[i] / input1[i]));
    eq.push_back(input0[i] == input1[i]);
    lt.push_back(input0[i] < input1[i]);
  }
  auto exp_sum = MakeArrowArray<TYPE, C_TYPE>(sum, validity);
  auto exp_sub = MakeArrowArray<TYPE, C_TYPE>(sub, validity);
  auto exp_mul = MakeArrowArray<TYPE, C_TYPE>(mul, validity);
  auto exp_div = MakeArrowArray<TYPE, C_TYPE>(div, validity);
  auto exp_eq = MakeArrowArray<arrow::BooleanType, bool>(eq, validity);
  auto exp_lt = MakeArrowArray<arrow::BooleanType, bool>(lt, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_sub, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(exp_mul, outputs.at(2));
  EXPECT_ARROW_ARRAY_EQUALS(exp_div, outputs.at(3));
  EXPECT_ARROW_ARRAY_EQUALS(exp_eq, outputs.at(4));
  EXPECT_ARROW_ARRAY_EQUALS(exp_lt, outputs.at(5));
}

TEST_F(TestProjector, TestAllIntTypes) {
  TestArithmeticOpsForType<arrow::UInt8Type, uint8_t>(pool_);
  TestArithmeticOpsForType<arrow::UInt16Type, uint16_t>(pool_);
  TestArithmeticOpsForType<arrow::UInt32Type, uint32_t>(pool_);
  TestArithmeticOpsForType<arrow::UInt64Type, uint64_t>(pool_);
  TestArithmeticOpsForType<arrow::Int8Type, int8_t>(pool_);
  TestArithmeticOpsForType<arrow::Int16Type, int16_t>(pool_);
  TestArithmeticOpsForType<arrow::Int32Type, int32_t>(pool_);
  TestArithmeticOpsForType<arrow::Int64Type, int64_t>(pool_);
}

TEST_F(TestProjector, TestExtendedMath) {
  // schema for input fields
  auto field0 = arrow::field("f0", arrow::float64());
  auto field1 = arrow::field("f1", arrow::float64());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_cbrt = arrow::field("cbrt", arrow::float64());
  auto field_exp = arrow::field("exp", arrow::float64());
  auto field_log = arrow::field("log", arrow::float64());
  auto field_log10 = arrow::field("log10", arrow::float64());
  auto field_logb = arrow::field("logb", arrow::float64());
  auto field_power = arrow::field("power", arrow::float64());

  // Build expression
  auto cbrt_expr = TreeExprBuilder::MakeExpression("cbrt", {field0}, field_cbrt);
  auto exp_expr = TreeExprBuilder::MakeExpression("exp", {field0}, field_exp);
  auto log_expr = TreeExprBuilder::MakeExpression("log", {field0}, field_log);
  auto log10_expr = TreeExprBuilder::MakeExpression("log10", {field0}, field_log10);
  auto logb_expr = TreeExprBuilder::MakeExpression("log", {field0, field1}, field_logb);
  auto power_expr =
      TreeExprBuilder::MakeExpression("power", {field0, field1}, field_power);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(
      schema, {cbrt_expr, exp_expr, log_expr, log10_expr, logb_expr, power_expr},
      TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  std::vector<double> input0 = {16, 10, -14, 8.3};
  std::vector<double> input1 = {2, 3, 5, 7};
  std::vector<bool> validity = {true, true, true, true};

  auto array0 = MakeArrowArray<arrow::DoubleType, double>(input0, validity);
  auto array1 = MakeArrowArray<arrow::DoubleType, double>(input1, validity);

  // expected output
  std::vector<double> cbrt_vals;
  std::vector<double> exp_vals;
  std::vector<double> log_vals;
  std::vector<double> log10_vals;
  std::vector<double> logb_vals;
  std::vector<double> power_vals;
  for (int i = 0; i < num_records; i++) {
    cbrt_vals.push_back(static_cast<double>(cbrtl(input0[i])));
    exp_vals.push_back(static_cast<double>(expl(input0[i])));
    log_vals.push_back(static_cast<double>(logl(input0[i])));
    log10_vals.push_back(static_cast<double>(log10l(input0[i])));
    logb_vals.push_back(static_cast<double>(logl(input1[i]) / logl(input0[i])));
    power_vals.push_back(static_cast<double>(powl(input0[i], input1[i])));
  }
  auto expected_cbrt = MakeArrowArray<arrow::DoubleType, double>(cbrt_vals, validity);
  auto expected_exp = MakeArrowArray<arrow::DoubleType, double>(exp_vals, validity);
  auto expected_log = MakeArrowArray<arrow::DoubleType, double>(log_vals, validity);
  auto expected_log10 = MakeArrowArray<arrow::DoubleType, double>(log10_vals, validity);
  auto expected_logb = MakeArrowArray<arrow::DoubleType, double>(logb_vals, validity);
  auto expected_power = MakeArrowArray<arrow::DoubleType, double>(power_vals, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  double epsilon = 1E-13;
  EXPECT_ARROW_ARRAY_APPROX_EQUALS(expected_cbrt, outputs.at(0), epsilon);
  EXPECT_ARROW_ARRAY_APPROX_EQUALS(expected_exp, outputs.at(1), epsilon);
  EXPECT_ARROW_ARRAY_APPROX_EQUALS(expected_log, outputs.at(2), epsilon);
  EXPECT_ARROW_ARRAY_APPROX_EQUALS(expected_log10, outputs.at(3), epsilon);
  EXPECT_ARROW_ARRAY_APPROX_EQUALS(expected_logb, outputs.at(4), epsilon);
  EXPECT_ARROW_ARRAY_APPROX_EQUALS(expected_power, outputs.at(5), epsilon);
}

TEST_F(TestProjector, TestFloatLessThan) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto field1 = field("f2", float32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_result = field("res", boolean());

  // Build expression
  auto lt_expr =
      TreeExprBuilder::MakeExpression("less_than", {field0, field1}, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {lt_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 3;
  auto array0 = MakeArrowArrayFloat32({1.0f, 8.9f, 3.0f}, {true, true, false});
  auto array1 = MakeArrowArrayFloat32({4.0f, 3.4f, 6.8f}, {true, true, true});
  // expected output
  auto exp = MakeArrowArrayBool({true, false, false}, {true, true, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestIsNotNull) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto schema = arrow::schema({field0});

  // output fields
  auto field_result = field("res", boolean());

  // Build expression
  auto myexpr = TreeExprBuilder::MakeExpression("isnotnull", {field0}, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {myexpr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 3;
  auto array0 = MakeArrowArrayFloat32({1.0f, 8.9f, 3.0f}, {true, true, false});
  // expected output
  auto exp = MakeArrowArrayBool({true, true, false}, {true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestZeroCopy) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // output fields
  auto res = field("res", float32());

  // Build expression
  auto cast_expr = TreeExprBuilder::MakeExpression("castFLOAT4", {field0}, res);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {cast_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, false});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // expected output
  auto exp = MakeArrowArrayFloat32({1, 2, 3, 0}, {true, true, true, false});

  // allocate output buffers
  int64_t bitmap_sz = arrow::BitUtil::BytesForBits(num_records);
  int64_t bitmap_capacity = arrow::BitUtil::RoundUpToMultipleOf64(bitmap_sz);
  std::vector<uint8_t> bitmap(bitmap_capacity);
  std::shared_ptr<arrow::MutableBuffer> bitmap_buf =
      std::make_shared<arrow::MutableBuffer>(&bitmap[0], bitmap_capacity);

  int64_t data_sz = sizeof(float) * num_records;
  std::vector<uint8_t> data(bitmap_capacity);
  std::shared_ptr<arrow::MutableBuffer> data_buf =
      std::make_shared<arrow::MutableBuffer>(&data[0], data_sz);

  auto array_data =
      arrow::ArrayData::Make(float32(), num_records, {bitmap_buf, data_buf});

  // Evaluate expression
  status = projector->Evaluate(*in_batch, {array_data});
  EXPECT_TRUE(status.ok());

  // Validate results
  auto output = arrow::MakeArray(array_data);
  EXPECT_ARROW_ARRAY_EQUALS(exp, output);
}

TEST_F(TestProjector, TestZeroCopyNegative) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // output fields
  auto res = field("res", float32());

  // Build expression
  auto cast_expr = TreeExprBuilder::MakeExpression("castFLOAT4", {field0}, res);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {cast_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, false});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // expected output
  auto exp = MakeArrowArrayFloat32({1, 2, 3, 0}, {true, true, true, false});

  // allocate output buffers
  int64_t bitmap_sz = arrow::BitUtil::BytesForBits(num_records);
  std::unique_ptr<uint8_t[]> bitmap(new uint8_t[bitmap_sz]);
  std::shared_ptr<arrow::MutableBuffer> bitmap_buf =
      std::make_shared<arrow::MutableBuffer>(bitmap.get(), bitmap_sz);

  int64_t data_sz = sizeof(float) * num_records;
  std::unique_ptr<uint8_t[]> data(new uint8_t[data_sz]);
  std::shared_ptr<arrow::MutableBuffer> data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz);

  auto array_data =
      arrow::ArrayData::Make(float32(), num_records, {bitmap_buf, data_buf});

  // the batch can't be empty.
  auto bad_batch = arrow::RecordBatch::Make(schema, 0 /*num_records*/, {array0});
  status = projector->Evaluate(*bad_batch, {array_data});
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output array can't be null.
  std::shared_ptr<arrow::ArrayData> null_array_data;
  status = projector->Evaluate(*in_batch, {null_array_data});
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output array must have at least two buffers.
  auto bad_array_data = arrow::ArrayData::Make(float32(), num_records, {bitmap_buf});
  status = projector->Evaluate(*in_batch, {bad_array_data});
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output buffers must have sufficiently sized data_buf.
  std::shared_ptr<arrow::MutableBuffer> bad_data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz - 1);
  auto bad_array_data2 =
      arrow::ArrayData::Make(float32(), num_records, {bitmap_buf, bad_data_buf});
  status = projector->Evaluate(*in_batch, {bad_array_data2});
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the output buffers must have sufficiently sized bitmap_buf.
  std::shared_ptr<arrow::MutableBuffer> bad_bitmap_buf =
      std::make_shared<arrow::MutableBuffer>(bitmap.get(), bitmap_sz - 1);
  auto bad_array_data3 =
      arrow::ArrayData::Make(float32(), num_records, {bad_bitmap_buf, data_buf});
  status = projector->Evaluate(*in_batch, {bad_array_data3});
  EXPECT_EQ(status.code(), StatusCode::Invalid);
}

TEST_F(TestProjector, TestDivideZero) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_div = field("divide", int32());

  // Build expression
  auto div_expr = TreeExprBuilder::MakeExpression("divide", {field0, field1}, field_div);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {div_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({2, 3, 4, 5, 6}, {true, true, true, true, true});
  auto array1 = MakeArrowArrayInt32({1, 2, 2, 0, 0}, {true, true, false, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_EQ(status.code(), StatusCode::ExecutionError);
  std::string expected_error = "divide by zero error";
  EXPECT_TRUE(status.message().find(expected_error) != std::string::npos);

  // Testing for second batch that has no error should succeed.
  num_records = 5;
  array0 = MakeArrowArrayInt32({2, 3, 4, 5, 6}, {true, true, true, true, true});
  array1 = MakeArrowArrayInt32({1, 2, 2, 1, 1}, {true, true, false, true, true});

  // prepare input record batch
  in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});
  // expected output
  auto exp = MakeArrowArrayInt32({2, 1, 2, 5, 6}, {true, true, false, true, true});

  // Evaluate expression
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestModZero) {
  // schema for input fields
  auto field0 = field("f0", arrow::int64());
  auto field1 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_div = field("mod", int32());

  // Build expression
  auto mod_expr = TreeExprBuilder::MakeExpression("mod", {field0, field1}, field_div);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {mod_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt64({2, 3, 4, 5}, {true, true, true, true});
  auto array1 = MakeArrowArrayInt32({1, 2, 2, 0}, {true, true, false, true});
  // expected output
  auto exp_mod = MakeArrowArrayInt32({0, 1, 0, 5}, {true, true, false, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_mod, outputs.at(0));
}

TEST_F(TestProjector, TestConcat) {
  // schema for input fields
  auto field0 = field("f0", arrow::utf8());
  auto field1 = field("f1", arrow::utf8());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_concat = field("concat", arrow::utf8());

  // Build expression
  auto concat_expr =
      TreeExprBuilder::MakeExpression("concat", {field0, field1}, field_concat);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {concat_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 6;
  auto array0 = MakeArrowArrayUtf8({"ab", "", "ab", "invalid", "valid", "invalid"},
                                   {true, true, true, false, true, false});
  auto array1 = MakeArrowArrayUtf8({"cd", "cd", "", "valid", "invalid", "invalid"},
                                   {true, true, true, true, false, false});
  // expected output
  auto exp_concat = MakeArrowArrayUtf8({"abcd", "cd", "ab", "valid", "valid", ""},
                                       {true, true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_concat, outputs.at(0));
}

TEST_F(TestProjector, TestOffset) {
  // schema for input fields
  auto field0 = field("f0", arrow::int32());
  auto field1 = field("f1", arrow::int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_sum = field("sum", arrow::int32());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {sum_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4, 5}, {true, true, true, true, false});
  array0 = array0->Slice(1);
  auto array1 = MakeArrowArrayInt32({5, 6, 7, 8}, {true, false, true, true});
  // expected output
  auto exp_sum = MakeArrowArrayInt32({9, 11, 13}, {false, true, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});
  in_batch = in_batch->Slice(1);

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
}

// Test to ensure behaviour of cast functions when the validity is false for an input. The
// function should not run for that input.
TEST_F(TestProjector, TestCastFunction) {
  auto field0 = field("f0", arrow::utf8());
  auto schema = arrow::schema({field0});

  // output fields
  auto res_float4 = field("res_float4", arrow::float32());
  auto res_float8 = field("res_float8", arrow::float64());
  auto res_int4 = field("castINT", arrow::int32());
  auto res_int8 = field("castBIGINT", arrow::int64());

  // Build expression
  auto cast_expr_float4 =
      TreeExprBuilder::MakeExpression("castFLOAT4", {field0}, res_float4);
  auto cast_expr_float8 =
      TreeExprBuilder::MakeExpression("castFLOAT8", {field0}, res_float8);
  auto cast_expr_int4 = TreeExprBuilder::MakeExpression("castINT", {field0}, res_int4);
  auto cast_expr_int8 = TreeExprBuilder::MakeExpression("castBIGINT", {field0}, res_int8);

  std::shared_ptr<Projector> projector;

  //  {cast_expr_float4, cast_expr_float8, cast_expr_int4, cast_expr_int8}
  auto status = Projector::Make(
      schema, {cast_expr_float4, cast_expr_float8, cast_expr_int4, cast_expr_int8},
      TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;

  // Last validity is false and the cast functions throw error when input is empty. Should
  // not be evaluated due to addition of NativeFunction::kCanReturnErrors
  auto array0 = MakeArrowArrayUtf8({"1", "2", "3", ""}, {true, true, true, false});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  auto out_float4 = MakeArrowArrayFloat32({1, 2, 3, 0}, {true, true, true, false});
  auto out_float8 = MakeArrowArrayFloat64({1, 2, 3, 0}, {true, true, true, false});
  auto out_int4 = MakeArrowArrayInt32({1, 2, 3, 0}, {true, true, true, false});
  auto out_int8 = MakeArrowArrayInt64({1, 2, 3, 0}, {true, true, true, false});

  arrow::ArrayVector outputs;

  // Evaluate expression
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  EXPECT_ARROW_ARRAY_EQUALS(out_float4, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(out_float8, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(out_int4, outputs.at(2));
  EXPECT_ARROW_ARRAY_EQUALS(out_int8, outputs.at(3));
}

TEST_F(TestProjector, TestToDate) {
  // schema for input fields
  auto field0 = field("f0", arrow::utf8());
  auto field_node = std::make_shared<FieldNode>(field0);
  auto schema = arrow::schema({field0});

  // output fields
  auto field_result = field("res", arrow::date64());

  auto pattern_node = std::make_shared<LiteralNode>(
      arrow::utf8(), LiteralHolder(std::string("YYYY-MM-DD")), false);

  // Build expression
  auto fn_node = TreeExprBuilder::MakeFunction("to_date", {field_node, pattern_node},
                                               arrow::date64());
  auto expr = TreeExprBuilder::MakeExpression(fn_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 3;
  auto array0 =
      MakeArrowArrayUtf8({"1986-12-01", "2012-12-01", "invalid"}, {true, true, false});
  // expected output
  auto exp = MakeArrowArrayDate64({533779200000, 1354320000000, 0}, {true, true, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestCastToUTF8) {
  // schema for input fields
  auto field_float64 = field("f_float64", arrow::float64());
  auto field_int32 = field("f_int32", arrow::int32());
  auto schema = arrow::schema({field_float64, field_int32});

  // output fields
  auto field_0 = field("float64_str", arrow::utf8());
  auto field_1 = field("int32_str", arrow::utf8());

  // Build expression
  auto node_a = TreeExprBuilder::MakeField(field_float64);
  auto node_b = TreeExprBuilder::MakeField(field_int32);
  auto int64_literal_0 = TreeExprBuilder::MakeLiteral(21L);
  auto int64_literal_1 = TreeExprBuilder::MakeLiteral(11L);
  auto func0 = TreeExprBuilder::MakeFunction("castVARCHAR", {node_a, int64_literal_0},
                                             arrow::utf8());
  auto expr0 = TreeExprBuilder::MakeExpression(func0, field_0);
  auto func1 = TreeExprBuilder::MakeFunction("castVARCHAR", {node_b, int64_literal_1},
                                             arrow::utf8());
  auto expr1 = TreeExprBuilder::MakeExpression(func1, field_1);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr0, expr1}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayFloat64({1989278888.23f, 5.892732f, -23487.3f, 9.712717f},
                                      {true, true, true, true});
  auto array1 = MakeArrowArrayInt32({5, 6, 7, 8}, {true, true, true, true});
  // expected output
  auto exp_0 = MakeArrowArray<arrow::StringType, std::string>(
      {"1.98928e+09", "5.89273", "-23487.3", "9.71272"}, {true, true, true, true});
  auto exp_1 = MakeArrowArray<arrow::StringType, std::string>({"5", "6", "7", "8"},
                                                              {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_0, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(1));
}

TEST_F(TestProjector, TestCastToByte) {
  // schema for input fields
  auto field_int16 = field("f_int16", arrow::int16());
  auto field_int64 = field("f_int64", arrow::int64());
  auto field_int32 = field("f_int32", arrow::int32());
  auto schema = arrow::schema({field_int16, field_int64, field_int32});

  // output fields
  auto field_0 = field("out_0", arrow::int8());
  auto field_1 = field("out_1", arrow::int8());
  auto field_2 = field("out_2", arrow::int8());

  // Build expression
  auto node_a = TreeExprBuilder::MakeField(field_int16);
  auto node_b = TreeExprBuilder::MakeField(field_int64);
  auto node_c = TreeExprBuilder::MakeField(field_int32);
  auto func0 = TreeExprBuilder::MakeFunction("castBYTE", {node_a}, arrow::int8());
  auto expr0 = TreeExprBuilder::MakeExpression(func0, field_0);
  auto func1 = TreeExprBuilder::MakeFunction("castBYTE", {node_b}, arrow::int8());
  auto expr1 = TreeExprBuilder::MakeExpression(func1, field_1);
  auto func2 = TreeExprBuilder::MakeFunction("castBYTE", {node_c}, arrow::int8());
  auto expr2 = TreeExprBuilder::MakeExpression(func2, field_2);

  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr0, expr1, expr2}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt16({5, 6, 7, 8}, {true, true, true, true});
  auto array1 = MakeArrowArrayInt64({5L, 6L, 7L, 257L}, {true, true, true, true});
  auto array2 = MakeArrowArrayInt32({5, 6, 7, 8}, {true, true, true, true});
  // expected output
  auto exp_0 = MakeArrowArrayInt8({5, 6, 7, 8}, {true, true, true, true});
  auto exp_1 = MakeArrowArrayInt8({5, 6, 7, 1}, {true, true, true, true});
  auto exp_2 = MakeArrowArrayInt8({5, 6, 7, 8}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_0, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs.at(2));
}
}  // namespace gandiva
