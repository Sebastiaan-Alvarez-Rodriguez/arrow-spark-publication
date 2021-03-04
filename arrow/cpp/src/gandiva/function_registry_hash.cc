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

#include "gandiva/function_registry_hash.h"
#include "gandiva/function_registry_common.h"

namespace gandiva {

#define HASH32_SAFE_NULL_NEVER_FN(name, ALIASES) \
  NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH32_SAFE_NULL_NEVER, name, ALIASES)

#define HASH32_SEED_SAFE_NULL_NEVER_FN(name, ALIASES) \
  NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH32_SEED_SAFE_NULL_NEVER, name, ALIASES)

#define HASH64_SAFE_NULL_NEVER_FN(name, ALIASES) \
  NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH64_SAFE_NULL_NEVER, name, ALIASES)

#define HASH64_SEED_SAFE_NULL_NEVER_FN(name, ALIASES) \
  NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH64_SEED_SAFE_NULL_NEVER, name, ALIASES)

std::vector<NativeFunction> GetHashFunctionRegistry() {
  static std::vector<NativeFunction> hash_fn_registry_ = {
      NativeFunction("hash32_spark", {}, DataTypeVector{boolean(), int32()}, int32(),
                     kResultNullNever, "hash32_spark_boolean_int32"),
      NativeFunction("hash32_spark", {}, DataTypeVector{int8(), int32()}, int32(),
                     kResultNullNever, "hash32_spark_int8_int32"),
      NativeFunction("hash32_spark", {}, DataTypeVector{int16(), int32()}, int32(),
                     kResultNullNever, "hash32_spark_int16_int32"),
      NativeFunction("hash32_spark", {}, DataTypeVector{int32(), int32()}, int32(),
                     kResultNullNever, "hash32_spark_int32_int32"),
      NativeFunction("hash32_spark", {}, DataTypeVector{date32(), int32()}, int32(),
                     kResultNullNever, "hash32_spark_date32_int32"),
      NativeFunction("hash32_spark", {}, DataTypeVector{float32(), int32()}, int32(),
                     kResultNullNever, "hash32_spark_float32_int32"),
      NativeFunction("hash64_spark", {}, DataTypeVector{int64(), int32()}, int32(),
                     kResultNullNever, "hash64_spark_int64_int32"),
      NativeFunction("hash64_spark", {}, DataTypeVector{float64(), int32()}, int32(),
                     kResultNullNever, "hash64_spark_float64_int32"),
      NativeFunction("hashbuf_spark", {}, DataTypeVector{utf8(), int32()}, int32(),
                     kResultNullNever, "hashbuf_spark_utf8_int32"),

      HASH32_SAFE_NULL_NEVER_FN(hash, {}),
      HASH32_SAFE_NULL_NEVER_FN(hash32, {}),
      HASH32_SAFE_NULL_NEVER_FN(hash32AsDouble, {}),

      HASH32_SEED_SAFE_NULL_NEVER_FN(hash32, {}),
      HASH32_SEED_SAFE_NULL_NEVER_FN(hash32AsDouble, {}),

      HASH64_SAFE_NULL_NEVER_FN(hash64, {}),
      HASH64_SAFE_NULL_NEVER_FN(hash64AsDouble, {}),

      HASH64_SEED_SAFE_NULL_NEVER_FN(hash64, {}),
      HASH64_SEED_SAFE_NULL_NEVER_FN(hash64AsDouble, {})};

  return hash_fn_registry_;
}

}  // namespace gandiva
