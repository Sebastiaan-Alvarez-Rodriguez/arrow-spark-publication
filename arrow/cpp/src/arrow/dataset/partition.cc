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

#include "arrow/dataset/partition.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_dict.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/scalar.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/string_view.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using util::string_view;

namespace dataset {

std::shared_ptr<Partitioning> Partitioning::Default() {
  class DefaultPartitioning : public Partitioning {
   public:
    DefaultPartitioning() : Partitioning(::arrow::schema({})) {}

    std::string type_name() const override { return "default"; }

    Result<Expression> Parse(const std::string& path) const override {
      return literal(true);
    }

    Result<std::string> Format(const Expression& expr) const override {
      return Status::NotImplemented("formatting paths from ", type_name(),
                                    " Partitioning");
    }

    Result<PartitionedBatches> Partition(
        const std::shared_ptr<RecordBatch>& batch) const override {
      return PartitionedBatches{{batch}, {literal(true)}};
    }
  };

  return std::make_shared<DefaultPartitioning>();
}

Status KeyValuePartitioning::SetDefaultValuesFromKeys(const Expression& expr,
                                                      RecordBatchProjector* projector) {
  ARROW_ASSIGN_OR_RAISE(auto known_values, ExtractKnownFieldValues(expr));
  for (const auto& ref_value : known_values) {
    if (!ref_value.second.is_scalar()) {
      return Status::Invalid("non-scalar partition key ", ref_value.second.ToString());
    }

    ARROW_ASSIGN_OR_RAISE(auto match,
                          ref_value.first.FindOneOrNone(*projector->schema()));

    if (match.empty()) continue;
    RETURN_NOT_OK(projector->SetDefaultValue(match, ref_value.second.scalar()));
  }
  return Status::OK();
}

inline Expression ConjunctionFromGroupingRow(Scalar* row) {
  ScalarVector* values = &checked_cast<StructScalar*>(row)->value;
  std::vector<Expression> equality_expressions(values->size());
  for (size_t i = 0; i < values->size(); ++i) {
    const std::string& name = row->type->field(static_cast<int>(i))->name();
    equality_expressions[i] = equal(field_ref(name), literal(std::move(values->at(i))));
  }
  return and_(std::move(equality_expressions));
}

Result<Partitioning::PartitionedBatches> KeyValuePartitioning::Partition(
    const std::shared_ptr<RecordBatch>& batch) const {
  FieldVector by_fields;
  ArrayVector by_columns;

  std::shared_ptr<RecordBatch> rest = batch;
  for (const auto& partition_field : schema_->fields()) {
    ARROW_ASSIGN_OR_RAISE(
        auto match, FieldRef(partition_field->name()).FindOneOrNone(*rest->schema()))
    if (match.empty()) continue;

    by_fields.push_back(partition_field);
    by_columns.push_back(rest->column(match[0]));
    ARROW_ASSIGN_OR_RAISE(rest, rest->RemoveColumn(match[0]));
  }

  if (by_fields.empty()) {
    // no fields to group by; return the whole batch
    return PartitionedBatches{{batch}, {literal(true)}};
  }

  ARROW_ASSIGN_OR_RAISE(auto by,
                        StructArray::Make(std::move(by_columns), std::move(by_fields)));
  ARROW_ASSIGN_OR_RAISE(auto groupings_and_values, MakeGroupings(*by));
  auto groupings =
      checked_pointer_cast<ListArray>(groupings_and_values->GetFieldByName("groupings"));
  auto unique_rows = groupings_and_values->GetFieldByName("values");

  PartitionedBatches out;
  ARROW_ASSIGN_OR_RAISE(out.batches, ApplyGroupings(*groupings, rest));
  out.expressions.resize(out.batches.size());

  for (size_t i = 0; i < out.batches.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto row, unique_rows->GetScalar(i));
    out.expressions[i] = ConjunctionFromGroupingRow(row.get());
  }
  return out;
}

Result<Expression> KeyValuePartitioning::ConvertKey(const Key& key) const {
  ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(key.name).FindOneOrNone(*schema_));
  if (match.empty()) {
    return literal(true);
  }

  auto field_index = match[0];
  auto field = schema_->field(field_index);

  std::shared_ptr<Scalar> converted;

  if (field->type()->id() == Type::DICTIONARY) {
    if (dictionaries_.empty() || dictionaries_[field_index] == nullptr) {
      return Status::Invalid("No dictionary provided for dictionary field ",
                             field->ToString());
    }

    DictionaryScalar::ValueType value;
    value.dictionary = dictionaries_[field_index];

    if (!value.dictionary->type()->Equals(
            checked_cast<const DictionaryType&>(*field->type()).value_type())) {
      return Status::TypeError("Dictionary supplied for field ", field->ToString(),
                               " had incorrect type ",
                               value.dictionary->type()->ToString());
    }

    // look up the partition value in the dictionary
    ARROW_ASSIGN_OR_RAISE(converted, Scalar::Parse(value.dictionary->type(), key.value));
    ARROW_ASSIGN_OR_RAISE(auto index, compute::IndexIn(converted, value.dictionary));
    value.index = index.scalar();
    if (!value.index->is_valid) {
      return Status::Invalid("Dictionary supplied for field ", field->ToString(),
                             " does not contain '", key.value, "'");
    }
    converted = std::make_shared<DictionaryScalar>(std::move(value), field->type());
  } else {
    ARROW_ASSIGN_OR_RAISE(converted, Scalar::Parse(field->type(), key.value));
  }

  return equal(field_ref(field->name()), literal(std::move(converted)));
}

Result<Expression> KeyValuePartitioning::Parse(const std::string& path) const {
  std::vector<Expression> expressions;

  for (const Key& key : ParseKeys(path)) {
    ARROW_ASSIGN_OR_RAISE(auto expr, ConvertKey(key));
    if (expr == literal(true)) continue;
    expressions.push_back(std::move(expr));
  }

  return and_(std::move(expressions));
}

Result<std::string> KeyValuePartitioning::Format(const Expression& expr) const {
  ScalarVector values{static_cast<size_t>(schema_->num_fields()), nullptr};

  ARROW_ASSIGN_OR_RAISE(auto known_values, ExtractKnownFieldValues(expr));
  for (const auto& ref_value : known_values) {
    if (!ref_value.second.is_scalar()) {
      return Status::Invalid("non-scalar partition key ", ref_value.second.ToString());
    }

    ARROW_ASSIGN_OR_RAISE(auto match, ref_value.first.FindOneOrNone(*schema_));
    if (match.empty()) continue;

    auto value = ref_value.second.scalar();

    const auto& field = schema_->field(match[0]);
    if (!value->type->Equals(field->type())) {
      return Status::TypeError("scalar ", value->ToString(), " (of type ", *value->type,
                               ") is invalid for ", field->ToString());
    }

    if (value->type->id() == Type::DICTIONARY) {
      ARROW_ASSIGN_OR_RAISE(
          value, checked_cast<const DictionaryScalar&>(*value).GetEncodedValue());
    }

    values[match[0]] = std::move(value);
  }

  return FormatValues(values);
}

std::vector<KeyValuePartitioning::Key> DirectoryPartitioning::ParseKeys(
    const std::string& path) const {
  std::vector<Key> keys;

  int i = 0;
  for (auto&& segment : fs::internal::SplitAbstractPath(path)) {
    if (i >= schema_->num_fields()) break;

    keys.push_back({schema_->field(i++)->name(), std::move(segment)});
  }

  return keys;
}

inline util::optional<int> NextValid(const ScalarVector& values, int first_null) {
  auto it = std::find_if(values.begin() + first_null + 1, values.end(),
                         [](const std::shared_ptr<Scalar>& v) { return v != nullptr; });

  if (it == values.end()) {
    return util::nullopt;
  }

  return static_cast<int>(it - values.begin());
}

Result<std::string> DirectoryPartitioning::FormatValues(
    const ScalarVector& values) const {
  std::vector<std::string> segments(static_cast<size_t>(schema_->num_fields()));

  for (int i = 0; i < schema_->num_fields(); ++i) {
    if (values[i] != nullptr) {
      segments[i] = values[i]->ToString();
      continue;
    }

    if (auto illegal_index = NextValid(values, i)) {
      // XXX maybe we should just ignore keys provided after the first absent one?
      return Status::Invalid("No partition key for ", schema_->field(i)->name(),
                             " but a key was provided subsequently for ",
                             schema_->field(*illegal_index)->name(), ".");
    }

    // if all subsequent keys are absent we'll just print the available keys
    break;
  }

  return fs::internal::JoinAbstractPath(std::move(segments));
}

class KeyValuePartitioningFactory : public PartitioningFactory {
 protected:
  explicit KeyValuePartitioningFactory(PartitioningFactoryOptions options)
      : options_(options) {}

  int GetOrInsertField(const std::string& name) {
    auto it_inserted =
        name_to_index_.emplace(name, static_cast<int>(name_to_index_.size()));

    if (it_inserted.second) {
      repr_memos_.push_back(MakeMemo());
    }

    return it_inserted.first->second;
  }

  Status InsertRepr(const std::string& name, util::string_view repr) {
    return InsertRepr(GetOrInsertField(name), repr);
  }

  Status InsertRepr(int index, util::string_view repr) {
    int dummy;
    return repr_memos_[index]->GetOrInsert<StringType>(repr, &dummy);
  }

  Result<std::shared_ptr<Schema>> DoInpsect() {
    dictionaries_.assign(name_to_index_.size(), nullptr);

    std::vector<std::shared_ptr<Field>> fields(name_to_index_.size());

    for (const auto& name_index : name_to_index_) {
      const auto& name = name_index.first;
      auto index = name_index.second;

      std::shared_ptr<ArrayData> reprs;
      RETURN_NOT_OK(repr_memos_[index]->GetArrayData(0, &reprs));

      if (reprs->length == 0) {
        return Status::Invalid("No segments were available for field '", name,
                               "'; couldn't infer type");
      }

      // try casting to int32, otherwise bail and just use the string reprs
      auto dict = compute::Cast(reprs, int32()).ValueOr(reprs).make_array();
      auto type = dict->type();
      if (options_.infer_dictionary) {
        // wrap the inferred type in dictionary()
        type = dictionary(int32(), std::move(type));
      }

      fields[index] = field(name, std::move(type));
      dictionaries_[index] = std::move(dict);
    }

    Reset();
    return ::arrow::schema(std::move(fields));
  }

  std::vector<std::string> FieldNames() {
    std::vector<std::string> names(name_to_index_.size());

    for (auto kv : name_to_index_) {
      names[kv.second] = kv.first;
    }
    return names;
  }

  virtual void Reset() {
    name_to_index_.clear();
    repr_memos_.clear();
  }

  std::unique_ptr<internal::DictionaryMemoTable> MakeMemo() {
    return internal::make_unique<internal::DictionaryMemoTable>(default_memory_pool(),
                                                                utf8());
  }

  PartitioningFactoryOptions options_;
  ArrayVector dictionaries_;
  std::unordered_map<std::string, int> name_to_index_;
  std::vector<std::unique_ptr<internal::DictionaryMemoTable>> repr_memos_;
};

class DirectoryPartitioningFactory : public KeyValuePartitioningFactory {
 public:
  DirectoryPartitioningFactory(std::vector<std::string> field_names,
                               PartitioningFactoryOptions options)
      : KeyValuePartitioningFactory(options), field_names_(std::move(field_names)) {
    Reset();
  }

  std::string type_name() const override { return "schema"; }

  Result<std::shared_ptr<Schema>> Inspect(
      const std::vector<std::string>& paths) override {
    for (auto path : paths) {
      size_t field_index = 0;
      for (auto&& segment : fs::internal::SplitAbstractPath(path)) {
        if (field_index == field_names_.size()) break;

        RETURN_NOT_OK(InsertRepr(static_cast<int>(field_index++), segment));
      }
    }

    return DoInpsect();
  }

  Result<std::shared_ptr<Partitioning>> Finish(
      const std::shared_ptr<Schema>& schema) const override {
    for (FieldRef ref : field_names_) {
      // ensure all of field_names_ are present in schema
      RETURN_NOT_OK(ref.FindOne(*schema).status());
    }

    // drop fields which aren't in field_names_
    auto out_schema = SchemaFromColumnNames(schema, field_names_);

    return std::make_shared<DirectoryPartitioning>(std::move(out_schema), dictionaries_);
  }

 private:
  void Reset() override {
    KeyValuePartitioningFactory::Reset();

    for (const auto& name : field_names_) {
      GetOrInsertField(name);
    }
  }

  std::vector<std::string> field_names_;
};

std::shared_ptr<PartitioningFactory> DirectoryPartitioning::MakeFactory(
    std::vector<std::string> field_names, PartitioningFactoryOptions options) {
  return std::shared_ptr<PartitioningFactory>(
      new DirectoryPartitioningFactory(std::move(field_names), options));
}

util::optional<KeyValuePartitioning::Key> HivePartitioning::ParseKey(
    const std::string& segment) {
  auto name_end = string_view(segment).find_first_of('=');
  if (name_end == string_view::npos) {
    return util::nullopt;
  }

  return Key{segment.substr(0, name_end), segment.substr(name_end + 1)};
}

std::vector<KeyValuePartitioning::Key> HivePartitioning::ParseKeys(
    const std::string& path) const {
  std::vector<Key> keys;

  for (const auto& segment : fs::internal::SplitAbstractPath(path)) {
    if (auto key = ParseKey(segment)) {
      keys.push_back(std::move(*key));
    }
  }

  return keys;
}

Result<std::string> HivePartitioning::FormatValues(const ScalarVector& values) const {
  std::vector<std::string> segments(static_cast<size_t>(schema_->num_fields()));

  for (int i = 0; i < schema_->num_fields(); ++i) {
    const std::string& name = schema_->field(i)->name();

    if (values[i] == nullptr) {
      if (!NextValid(values, i)) break;

      // If no key is available just provide a placeholder segment to maintain the
      // field_index <-> path nesting relation
      segments[i] = name;
    } else {
      segments[i] = name + "=" + values[i]->ToString();
    }
  }

  return fs::internal::JoinAbstractPath(std::move(segments));
}

class HivePartitioningFactory : public KeyValuePartitioningFactory {
 public:
  explicit HivePartitioningFactory(PartitioningFactoryOptions options)
      : KeyValuePartitioningFactory(options) {}

  std::string type_name() const override { return "hive"; }

  Result<std::shared_ptr<Schema>> Inspect(
      const std::vector<std::string>& paths) override {
    for (auto path : paths) {
      for (auto&& segment : fs::internal::SplitAbstractPath(path)) {
        if (auto key = HivePartitioning::ParseKey(segment)) {
          RETURN_NOT_OK(InsertRepr(key->name, key->value));
        }
      }
    }

    field_names_ = FieldNames();
    return DoInpsect();
  }

  Result<std::shared_ptr<Partitioning>> Finish(
      const std::shared_ptr<Schema>& schema) const override {
    if (dictionaries_.empty()) {
      return std::make_shared<HivePartitioning>(schema, dictionaries_);
    } else {
      for (FieldRef ref : field_names_) {
        // ensure all of field_names_ are present in schema
        RETURN_NOT_OK(ref.FindOne(*schema));
      }

      // drop fields which aren't in field_names_
      auto out_schema = SchemaFromColumnNames(schema, field_names_);

      return std::make_shared<HivePartitioning>(std::move(out_schema), dictionaries_);
    }
  }

 private:
  std::vector<std::string> field_names_;
};

std::shared_ptr<PartitioningFactory> HivePartitioning::MakeFactory(
    PartitioningFactoryOptions options) {
  return std::shared_ptr<PartitioningFactory>(new HivePartitioningFactory(options));
}

std::string StripPrefixAndFilename(const std::string& path, const std::string& prefix) {
  auto maybe_base_less = fs::internal::RemoveAncestor(prefix, path);
  auto base_less = maybe_base_less ? std::string(*maybe_base_less) : path;
  auto basename_filename = fs::internal::GetAbstractPathParent(base_less);
  return basename_filename.first;
}

std::vector<std::string> StripPrefixAndFilename(const std::vector<std::string>& paths,
                                                const std::string& prefix) {
  std::vector<std::string> result;
  result.reserve(paths.size());
  for (const auto& path : paths) {
    result.emplace_back(StripPrefixAndFilename(path, prefix));
  }
  return result;
}

std::vector<std::string> StripPrefixAndFilename(const std::vector<fs::FileInfo>& files,
                                                const std::string& prefix) {
  std::vector<std::string> result;
  result.reserve(files.size());
  for (const auto& info : files) {
    result.emplace_back(StripPrefixAndFilename(info.path(), prefix));
  }
  return result;
}

Result<std::shared_ptr<Schema>> PartitioningOrFactory::GetOrInferSchema(
    const std::vector<std::string>& paths) {
  if (auto part = partitioning()) {
    return part->schema();
  }

  return factory()->Inspect(paths);
}

// Transform an array of counts to offsets which will divide a ListArray
// into an equal number of slices with corresponding lengths.
inline Result<std::shared_ptr<Buffer>> CountsToOffsets(
    std::shared_ptr<Int64Array> counts) {
  TypedBufferBuilder<int32_t> offset_builder;
  RETURN_NOT_OK(offset_builder.Resize(counts->length() + 1));

  int32_t current_offset = 0;
  offset_builder.UnsafeAppend(current_offset);

  for (int64_t i = 0; i < counts->length(); ++i) {
    DCHECK_NE(counts->Value(i), 0);
    current_offset += static_cast<int32_t>(counts->Value(i));
    offset_builder.UnsafeAppend(current_offset);
  }

  std::shared_ptr<Buffer> offsets;
  RETURN_NOT_OK(offset_builder.Finish(&offsets));
  return offsets;
}

// Helper for simultaneous dictionary encoding of multiple arrays.
//
// The fused dictionary is the Cartesian product of the individual dictionaries.
// For example given two arrays A, B where A has unique values ["ex", "why"]
// and B has unique values [0, 1] the fused dictionary is the set of tuples
// [["ex", 0], ["ex", 1], ["why", 0], ["ex", 1]].
//
// TODO(bkietz) this capability belongs in an Action of the hash kernels, where
// it can be used to group aggregates without materializing a grouped batch.
// For the purposes of writing we need the materialized grouped batch anyway
// since no Writers accept a selection vector.
class StructDictionary {
 public:
  struct Encoded {
    std::shared_ptr<Int32Array> indices;
    std::shared_ptr<StructDictionary> dictionary;
  };

  static Result<Encoded> Encode(const ArrayVector& columns) {
    Encoded out{nullptr, std::make_shared<StructDictionary>()};

    for (const auto& column : columns) {
      if (column->null_count() != 0) {
        return Status::NotImplemented("Grouping on a field with nulls");
      }

      RETURN_NOT_OK(out.dictionary->AddOne(column, &out.indices));
    }

    return out;
  }

  Result<std::shared_ptr<StructArray>> Decode(std::shared_ptr<Int32Array> fused_indices,
                                              FieldVector fields) {
    std::vector<Int32Builder> builders(dictionaries_.size());
    for (Int32Builder& b : builders) {
      RETURN_NOT_OK(b.Resize(fused_indices->length()));
    }

    std::vector<int32_t> codes(dictionaries_.size());
    for (int64_t i = 0; i < fused_indices->length(); ++i) {
      Expand(fused_indices->Value(i), codes.data());

      auto builder_it = builders.begin();
      for (int32_t index : codes) {
        builder_it++->UnsafeAppend(index);
      }
    }

    ArrayVector columns(dictionaries_.size());
    for (size_t i = 0; i < dictionaries_.size(); ++i) {
      std::shared_ptr<ArrayData> indices;
      RETURN_NOT_OK(builders[i].FinishInternal(&indices));

      ARROW_ASSIGN_OR_RAISE(Datum column, compute::Take(dictionaries_[i], indices));

      if (fields[i]->type()->id() == Type::DICTIONARY) {
        RETURN_NOT_OK(RestoreDictionaryEncoding(
            checked_pointer_cast<DictionaryType>(fields[i]->type()), &column));
      }

      columns[i] = column.make_array();
    }

    return StructArray::Make(std::move(columns), std::move(fields));
  }

 private:
  Status AddOne(Datum column, std::shared_ptr<Int32Array>* fused_indices) {
    if (column.type()->id() != Type::DICTIONARY) {
      ARROW_ASSIGN_OR_RAISE(column, compute::DictionaryEncode(std::move(column)));
    }

    auto dict_column = column.array_as<DictionaryArray>();
    dictionaries_.push_back(dict_column->dictionary());
    ARROW_ASSIGN_OR_RAISE(auto indices, compute::Cast(*dict_column->indices(), int32()));

    if (*fused_indices == nullptr) {
      *fused_indices = checked_pointer_cast<Int32Array>(std::move(indices));
      return IncreaseSize();
    }

    // It's useful to think about the case where each of dictionaries_ has size 10.
    // In this case the decimal digit in the ones place is the code in dictionaries_[0],
    // the tens place corresponds to the code in dictionaries_[1], etc.
    // The incumbent indices must be shifted to the hundreds place so as not to collide.
    ARROW_ASSIGN_OR_RAISE(Datum new_fused_indices,
                          compute::Multiply(indices, MakeScalar(size_)));

    ARROW_ASSIGN_OR_RAISE(new_fused_indices,
                          compute::Add(new_fused_indices, *fused_indices));

    *fused_indices = checked_pointer_cast<Int32Array>(new_fused_indices.make_array());
    return IncreaseSize();
  }

  // expand a fused code into component dict codes, order is in order of addition
  void Expand(int32_t fused_code, int32_t* codes) {
    for (size_t i = 0; i < dictionaries_.size(); ++i) {
      auto dictionary_size = static_cast<int32_t>(dictionaries_[i]->length());
      codes[i] = fused_code % dictionary_size;
      fused_code /= dictionary_size;
    }
  }

  Status RestoreDictionaryEncoding(std::shared_ptr<DictionaryType> expected_type,
                                   Datum* column) {
    DCHECK_NE(column->type()->id(), Type::DICTIONARY);
    ARROW_ASSIGN_OR_RAISE(*column, compute::DictionaryEncode(std::move(*column)));

    if (expected_type->index_type()->id() == Type::INT32) {
      // dictionary_encode has already yielded the expected index_type
      return Status::OK();
    }

    // cast the indices to the expected index type
    auto dictionary = std::move(column->mutable_array()->dictionary);
    column->mutable_array()->type = int32();

    ARROW_ASSIGN_OR_RAISE(*column,
                          compute::Cast(std::move(*column), expected_type->index_type()));

    column->mutable_array()->dictionary = std::move(dictionary);
    column->mutable_array()->type = expected_type;
    return Status::OK();
  }

  Status IncreaseSize() {
    auto factor = static_cast<int32_t>(dictionaries_.back()->length());

    if (internal::MultiplyWithOverflow(size_, factor, &size_)) {
      return Status::CapacityError("Max groups exceeded");
    }
    return Status::OK();
  }

  int32_t size_ = 1;
  ArrayVector dictionaries_;
};

Result<std::shared_ptr<StructArray>> MakeGroupings(const StructArray& by) {
  if (by.num_fields() == 0) {
    return Status::Invalid("Grouping with no criteria");
  }

  if (by.null_count() != 0) {
    return Status::Invalid("Grouping with null criteria");
  }

  ARROW_ASSIGN_OR_RAISE(auto fused, StructDictionary::Encode(by.fields()));

  ARROW_ASSIGN_OR_RAISE(auto sort_indices, compute::SortIndices(*fused.indices));
  ARROW_ASSIGN_OR_RAISE(Datum sorted, compute::Take(fused.indices, *sort_indices));
  fused.indices = checked_pointer_cast<Int32Array>(sorted.make_array());

  ARROW_ASSIGN_OR_RAISE(auto fused_counts_and_values,
                        compute::ValueCounts(fused.indices));
  fused.indices.reset();

  auto unique_fused_indices =
      checked_pointer_cast<Int32Array>(fused_counts_and_values->GetFieldByName("values"));
  ARROW_ASSIGN_OR_RAISE(
      auto unique_rows,
      fused.dictionary->Decode(std::move(unique_fused_indices), by.type()->fields()));

  auto counts =
      checked_pointer_cast<Int64Array>(fused_counts_and_values->GetFieldByName("counts"));
  ARROW_ASSIGN_OR_RAISE(auto offsets, CountsToOffsets(std::move(counts)));

  auto grouped_sort_indices =
      std::make_shared<ListArray>(list(sort_indices->type()), unique_rows->length(),
                                  std::move(offsets), std::move(sort_indices));

  return StructArray::Make(
      ArrayVector{std::move(unique_rows), std::move(grouped_sort_indices)},
      std::vector<std::string>{"values", "groupings"});
}

Result<std::shared_ptr<ListArray>> ApplyGroupings(const ListArray& groupings,
                                                  const Array& array) {
  ARROW_ASSIGN_OR_RAISE(Datum sorted,
                        compute::Take(array, groupings.data()->child_data[0]));

  return std::make_shared<ListArray>(list(array.type()), groupings.length(),
                                     groupings.value_offsets(), sorted.make_array());
}

Result<RecordBatchVector> ApplyGroupings(const ListArray& groupings,
                                         const std::shared_ptr<RecordBatch>& batch) {
  ARROW_ASSIGN_OR_RAISE(Datum sorted,
                        compute::Take(batch, groupings.data()->child_data[0]));

  const auto& sorted_batch = *sorted.record_batch();

  RecordBatchVector out(static_cast<size_t>(groupings.length()));
  for (size_t i = 0; i < out.size(); ++i) {
    out[i] = sorted_batch.Slice(groupings.value_offset(i), groupings.value_length(i));
  }

  return out;
}

}  // namespace dataset
}  // namespace arrow
