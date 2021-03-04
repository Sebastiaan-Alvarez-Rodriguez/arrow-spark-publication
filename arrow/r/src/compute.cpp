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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)

#include <arrow/compute/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>

arrow::compute::ExecContext* gc_context() {
  static arrow::compute::ExecContext context(gc_memory_pool());
  return &context;
}

// [[arrow::export]]
std::shared_ptr<arrow::compute::CastOptions> compute___CastOptions__initialize(
    bool allow_int_overflow, bool allow_time_truncate, bool allow_float_truncate) {
  auto options = std::make_shared<arrow::compute::CastOptions>();
  options->allow_int_overflow = allow_int_overflow;
  options->allow_time_truncate = allow_time_truncate;
  options->allow_float_truncate = allow_float_truncate;
  return options;
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__cast(
    const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<arrow::DataType>& target_type,
    const std::shared_ptr<arrow::compute::CastOptions>& options) {
  return ValueOrStop(arrow::compute::Cast(*array, target_type, *options, gc_context()));
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__cast(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const std::shared_ptr<arrow::DataType>& target_type,
    const std::shared_ptr<arrow::compute::CastOptions>& options) {
  arrow::Datum value(chunked_array);
  arrow::Datum out =
      ValueOrStop(arrow::compute::Cast(value, target_type, *options, gc_context()));
  return out.chunked_array();
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__cast(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::compute::CastOptions>& options) {
  auto nc = batch->num_columns();

  arrow::ArrayVector columns(nc);
  for (int i = 0; i < nc; i++) {
    columns[i] = ValueOrStop(
        arrow::compute::Cast(*batch->column(i), schema->field(i)->type(), *options));
  }

  return arrow::RecordBatch::Make(schema, batch->num_rows(), std::move(columns));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__cast(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::compute::CastOptions>& options) {
  auto nc = table->num_columns();

  using ColumnVector = std::vector<std::shared_ptr<arrow::ChunkedArray>>;
  ColumnVector columns(nc);
  for (int i = 0; i < nc; i++) {
    arrow::Datum value(table->column(i));
    arrow::Datum out =
        ValueOrStop(arrow::compute::Cast(value, schema->field(i)->type(), *options));
    columns[i] = out.chunked_array();
  }
  return arrow::Table::Make(schema, std::move(columns), table->num_rows());
}

template <typename T>
std::shared_ptr<T> MaybeUnbox(const char* class_name, SEXP x) {
  if (Rf_inherits(x, "ArrowObject") && Rf_inherits(x, class_name)) {
    return cpp11::as_cpp<std::shared_ptr<T>>(x);
  }
  return nullptr;
}

namespace cpp11 {

template <>
arrow::Datum as_cpp<arrow::Datum>(SEXP x) {
  if (auto array = MaybeUnbox<arrow::Array>("Array", x)) {
    return array;
  }

  if (auto chunked_array = MaybeUnbox<arrow::ChunkedArray>("ChunkedArray", x)) {
    return chunked_array;
  }

  if (auto batch = MaybeUnbox<arrow::RecordBatch>("RecordBatch", x)) {
    return batch;
  }

  if (auto table = MaybeUnbox<arrow::Table>("Table", x)) {
    return table;
  }

  if (auto scalar = MaybeUnbox<arrow::Scalar>("Scalar", x)) {
    return scalar;
  }

  // This assumes that R objects have already been converted to Arrow objects;
  // that seems right but should we do the wrapping here too/instead?
  cpp11::stop("to_datum: Not implemented for type %s", Rf_type2char(TYPEOF(x)));
}
}  // namespace cpp11

SEXP from_datum(arrow::Datum datum) {
  switch (datum.kind()) {
    case arrow::Datum::SCALAR:
      return cpp11::to_r6(datum.scalar());

    case arrow::Datum::ARRAY:
      return cpp11::to_r6(datum.make_array());

    case arrow::Datum::CHUNKED_ARRAY:
      return cpp11::to_r6(datum.chunked_array());

    case arrow::Datum::RECORD_BATCH:
      return cpp11::to_r6(datum.record_batch());

    case arrow::Datum::TABLE:
      return cpp11::to_r6(datum.table());

    default:
      break;
  }

  cpp11::stop("from_datum: Not implemented for Datum %s", datum.ToString().c_str());
}

std::shared_ptr<arrow::compute::FunctionOptions> make_compute_options(
    std::string func_name, cpp11::list options) {
  if (func_name == "filter") {
    using Options = arrow::compute::FilterOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    SEXP keep_na = options["keep_na"];
    if (!Rf_isNull(keep_na) && cpp11::as_cpp<bool>(keep_na)) {
      out->null_selection_behavior = Options::EMIT_NULL;
    }
    return out;
  }

  if (func_name == "take") {
    using Options = arrow::compute::TakeOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    return out;
  }

  if (func_name == "min_max") {
    using Options = arrow::compute::MinMaxOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    out->null_handling =
        cpp11::as_cpp<bool>(options["na.rm"]) ? Options::SKIP : Options::EMIT_NULL;
    return out;
  }

  if (func_name == "is_in" || func_name == "index_in") {
    using Options = arrow::compute::SetLookupOptions;
    return std::make_shared<Options>(cpp11::as_cpp<arrow::Datum>(options["value_set"]),
                                     cpp11::as_cpp<bool>(options["skip_nulls"]));
  }

  // hacky attempt to pass through to_type and other options
  if (func_name == "cast") {
    using Options = arrow::compute::CastOptions;
    auto out = std::make_shared<Options>(true);
    SEXP to_type = options["to_type"];
    if (!Rf_isNull(to_type) && cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(to_type)) {
      out->to_type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(to_type);
    }

    SEXP allow_float_truncate = options["allow_float_truncate"];
    if (!Rf_isNull(allow_float_truncate) && cpp11::as_cpp<bool>(allow_float_truncate)) {
      out->allow_float_truncate = cpp11::as_cpp<bool>(allow_float_truncate);
    }

    SEXP allow_time_truncate = options["allow_time_truncate"];
    if (!Rf_isNull(allow_time_truncate) && cpp11::as_cpp<bool>(allow_time_truncate)) {
      out->allow_time_truncate = cpp11::as_cpp<bool>(allow_time_truncate);
    }

    SEXP allow_int_overflow = options["allow_int_overflow"];
    if (!Rf_isNull(allow_int_overflow) && cpp11::as_cpp<bool>(allow_int_overflow)) {
      out->allow_int_overflow = cpp11::as_cpp<bool>(allow_int_overflow);
    }

    return out;
  }

  return nullptr;
}

// [[arrow::export]]
SEXP compute__CallFunction(std::string func_name, cpp11::list args, cpp11::list options) {
  auto opts = make_compute_options(func_name, options);
  auto datum_args = arrow::r::from_r_list<arrow::Datum>(args);
  auto out = ValueOrStop(
      arrow::compute::CallFunction(func_name, datum_args, opts.get(), gc_context()));
  return from_datum(std::move(out));
}

#endif
