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

extern "C" {

#include <math.h>
#include <stdio.h>
#include <string.h>
#include <cfloat>
#include "./types.h"

// Expand inner macro for all numeric types.
#define NUMERIC_TYPES(INNER, NAME, OP) \
  INNER(NAME, int8, OP)                \
  INNER(NAME, int16, OP)               \
  INNER(NAME, int32, OP)               \
  INNER(NAME, int64, OP)               \
  INNER(NAME, uint8, OP)               \
  INNER(NAME, uint16, OP)              \
  INNER(NAME, uint32, OP)              \
  INNER(NAME, uint64, OP)              \
  INNER(NAME, float32, OP)             \
  INNER(NAME, float64, OP)

// Expand inner macros for all date/time types.
#define DATE_TYPES(INNER, NAME, OP) \
  INNER(NAME, date64, OP)           \
  INNER(NAME, date32, OP)           \
  INNER(NAME, timestamp, OP)        \
  INNER(NAME, time32, OP)

#define NUMERIC_DATE_TYPES(INNER, NAME, OP) \
  NUMERIC_TYPES(INNER, NAME, OP)            \
  DATE_TYPES(INNER, NAME, OP)

#define NUMERIC_BOOL_DATE_TYPES(INNER, NAME, OP) \
  NUMERIC_TYPES(INNER, NAME, OP)                 \
  DATE_TYPES(INNER, NAME, OP)                    \
  INNER(NAME, boolean, OP)

#define MOD_OP(NAME, IN_TYPE1, IN_TYPE2, OUT_TYPE)                      \
  FORCE_INLINE                                                          \
  gdv_##OUT_TYPE NAME##_##IN_TYPE1##_##IN_TYPE2(gdv_##IN_TYPE1 left,    \
                                                gdv_##IN_TYPE2 right) { \
    return (right == 0 ? static_cast<gdv_##OUT_TYPE>(left)              \
                       : static_cast<gdv_##OUT_TYPE>(left % right));    \
  }

// Symmetric binary fns : left, right params and return type are same.
#define BINARY_SYMMETRIC(NAME, TYPE, OP)                                 \
  FORCE_INLINE                                                           \
  gdv_##TYPE NAME##_##TYPE##_##TYPE(gdv_##TYPE left, gdv_##TYPE right) { \
    return static_cast<gdv_##TYPE>(left OP right);                       \
  }

NUMERIC_TYPES(BINARY_SYMMETRIC, add, +)
NUMERIC_TYPES(BINARY_SYMMETRIC, subtract, -)
NUMERIC_TYPES(BINARY_SYMMETRIC, multiply, *)
BINARY_SYMMETRIC(bitwise_and, int32, &)
BINARY_SYMMETRIC(bitwise_and, int64, &)
BINARY_SYMMETRIC(bitwise_or, int32, |)
BINARY_SYMMETRIC(bitwise_or, int64, |)
BINARY_SYMMETRIC(bitwise_xor, int32, ^)
BINARY_SYMMETRIC(bitwise_xor, int64, ^)

#undef BINARY_SYMMETRIC

FORCE_INLINE
gdv_boolean isNaN_float32(gdv_float32 val) { return isnan(val) || isinf(val); }

FORCE_INLINE
gdv_boolean isNaN_float64(gdv_float64 val) { return isnan(val) || isinf(val); }

MOD_OP(mod, int64, int32, int32)
MOD_OP(mod, int64, int64, int64)

#undef MOD_OP

gdv_float64 mod_float64_float64(int64_t context, gdv_float64 x, gdv_float64 y) {
  if (y == 0.0) {
    char const* err_msg = "divide by zero error";
    gdv_fn_context_set_error_msg(context, err_msg);
    return 0.0;
  }
  return fmod(x, y);
}

// Relational binary fns : left, right params are same, return is bool.
#define BINARY_RELATIONAL(NAME, TYPE, OP) \
  FORCE_INLINE                            \
  bool NAME##_##TYPE##_##TYPE(gdv_##TYPE left, gdv_##TYPE right) { return left OP right; }

NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL, equal, ==)
NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL, not_equal, !=)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, less_than, <)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, less_than_or_equal_to, <=)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, greater_than, >)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, greater_than_or_equal_to, >=)

#undef BINARY_RELATIONAL

// Relational binary fns : left, right params are same, return is bool.
#define BINARY_RELATIONAL_NAN(NAME, TYPE, OP)                       \
  FORCE_INLINE                                                      \
  bool NAME##_##TYPE##_##TYPE(gdv_##TYPE left, gdv_##TYPE right) {  \
    const double infinity = 1.0 / 0.0;                              \
    bool left_is_nan = isnan(left);                                 \
    bool right_is_nan = isnan(right);                               \
    if (left_is_nan && right_is_nan) {                              \
      return infinity OP infinity;                                  \
    } else if (left_is_nan) {                                       \
      return infinity OP right;                                     \
    } else if (right_is_nan) {                                      \
      return left OP infinity;                                      \
    }                                                               \
    return left OP right;                                           \
  }

NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL_NAN, equal_with_nan, ==)
NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL_NAN, not_equal_with_nan, !=)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL_NAN, less_than_with_nan, <)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL_NAN, less_than_or_equal_to_with_nan, <=)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL_NAN, greater_than_with_nan, >)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL_NAN, greater_than_or_equal_to_with_nan, >=)

#undef BINARY_RELATIONAL_NAN

// cast fns : takes one param type, returns another type.
#define CAST_UNARY(NAME, IN_TYPE, OUT_TYPE)           \
  FORCE_INLINE                                        \
  gdv_##OUT_TYPE NAME##_##IN_TYPE(gdv_##IN_TYPE in) { \
    return static_cast<gdv_##OUT_TYPE>(in);           \
  }

CAST_UNARY(castBIGINT, int32, int64)
CAST_UNARY(castBIGINT, date64, int64)
CAST_UNARY(castBIGINT, float32, int64)
CAST_UNARY(castBIGINT, float64, int64)
CAST_UNARY(castINT, int8, int32)
CAST_UNARY(castINT, int16, int32)
CAST_UNARY(castINT, int64, int32)
CAST_UNARY(castINT, date32, int32)
CAST_UNARY(castINT, float32, int32)
CAST_UNARY(castINT, float64, int32)
CAST_UNARY(castBYTE, int16, int8)
CAST_UNARY(castBYTE, int32, int8)
CAST_UNARY(castBYTE, int64, int8)
CAST_UNARY(castFLOAT4, int32, float32)
CAST_UNARY(castFLOAT4, int64, float32)
CAST_UNARY(castFLOAT8, int32, float64)
CAST_UNARY(castFLOAT8, int64, float64)
CAST_UNARY(castFLOAT8, float32, float64)
CAST_UNARY(castFLOAT4, float64, float32)

#undef CAST_UNARY
#define nothing
#define PRINT(DIGSF, DIGS, FMT) PRINT_##DIGSF(DIGS, FMT)
#define PRINT_NOFMT(DIGS, FMT) int res = snprintf(char_buffer, length, FMT, in);
#define PRINT_FMT(DIGS, FMT) int res = snprintf(char_buffer, length, FMT, DIGS, in);

#define CAST_UNARY_UTF8(NAME, IN_TYPE, OUT_TYPE, FMT, DIGSF, DIGS)                       \
  FORCE_INLINE                                                                           \
  const char* NAME##_##IN_TYPE##_int64(gdv_int64 context, gdv_##IN_TYPE in,              \
                                       gdv_int64 length, gdv_int32 * out_len) {          \
    const int32_t char_buffer_length = length;                                           \
    char char_buffer[char_buffer_length];                                                \
    PRINT(DIGSF, DIGS, FMT)                                                              \
    if (res < 0) {                                                                       \
      gdv_fn_context_set_error_msg(context, "Could not format the ##IN_TYPE");           \
      return "";                                                                         \
    }                                                                                    \
                                                                                         \
    *out_len = strlen(char_buffer);                                                      \
    char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len)); \
    if (ret == nullptr) {                                                                \
      gdv_fn_context_set_error_msg(context,                                              \
                                   "Could not allocate memory for output string");       \
      *out_len = 0;                                                                      \
      return "";                                                                         \
    }                                                                                    \
                                                                                         \
    memcpy(ret, char_buffer, *out_len);                                                  \
    return ret;                                                                          \
  }

CAST_UNARY_UTF8(castVARCHAR, int8, utf8, "%d", NOFMT, nothing)
CAST_UNARY_UTF8(castVARCHAR, int16, utf8, "%d", NOFMT, nothing)
CAST_UNARY_UTF8(castVARCHAR, int32, utf8, "%d", NOFMT, nothing)
CAST_UNARY_UTF8(castVARCHAR, int64, utf8, "%ld", NOFMT, nothing)
// CAST_UNARY_UTF8(castVARCHAR, float32, utf8, "%.*f", FMT, FLT_DIG)
// CAST_UNARY_UTF8(castVARCHAR, float64, utf8, "%.*f", FMT, DBL_DIG)
CAST_UNARY_UTF8(castVARCHAR, float32, utf8, "%g", NOFMT, nothing)
CAST_UNARY_UTF8(castVARCHAR, float64, utf8, "%g", NOFMT, nothing)

#undef CAST_UNARY_UTF8

// simple nullable functions, result value = fn(input validity)
#define VALIDITY_OP(NAME, TYPE, OP) \
  FORCE_INLINE                      \
  bool NAME##_##TYPE(gdv_##TYPE in, gdv_boolean is_valid) { return OP is_valid; }

NUMERIC_BOOL_DATE_TYPES(VALIDITY_OP, isnull, !)
NUMERIC_BOOL_DATE_TYPES(VALIDITY_OP, isnotnull, +)
NUMERIC_TYPES(VALIDITY_OP, isnumeric, +)

#undef VALIDITY_OP

#define NUMERIC_FUNCTION(INNER) \
  INNER(int8)                   \
  INNER(int16)                  \
  INNER(int32)                  \
  INNER(int64)                  \
  INNER(uint8)                  \
  INNER(uint16)                 \
  INNER(uint32)                 \
  INNER(uint64)                 \
  INNER(float32)                \
  INNER(float64)

#define DATE_FUNCTION(INNER) \
  INNER(date32)              \
  INNER(date64)              \
  INNER(timestamp)           \
  INNER(time32)

#define NUMERIC_BOOL_DATE_FUNCTION(INNER) \
  NUMERIC_FUNCTION(INNER)                 \
  DATE_FUNCTION(INNER)                    \
  INNER(boolean)

FORCE_INLINE
gdv_boolean not_boolean(gdv_boolean in) { return !in; }

// is_distinct_from
#define IS_DISTINCT_FROM(TYPE)                                                   \
  FORCE_INLINE                                                                   \
  bool is_distinct_from_##TYPE##_##TYPE(gdv_##TYPE in1, gdv_boolean is_valid1,   \
                                        gdv_##TYPE in2, gdv_boolean is_valid2) { \
    if (is_valid1 != is_valid2) {                                                \
      return true;                                                               \
    }                                                                            \
    if (!is_valid1) {                                                            \
      return false;                                                              \
    }                                                                            \
    return in1 != in2;                                                           \
  }

// is_not_distinct_from
#define IS_NOT_DISTINCT_FROM(TYPE)                                                   \
  FORCE_INLINE                                                                       \
  bool is_not_distinct_from_##TYPE##_##TYPE(gdv_##TYPE in1, gdv_boolean is_valid1,   \
                                            gdv_##TYPE in2, gdv_boolean is_valid2) { \
    if (is_valid1 != is_valid2) {                                                    \
      return false;                                                                  \
    }                                                                                \
    if (!is_valid1) {                                                                \
      return true;                                                                   \
    }                                                                                \
    return in1 == in2;                                                               \
  }

NUMERIC_BOOL_DATE_FUNCTION(IS_DISTINCT_FROM)
NUMERIC_BOOL_DATE_FUNCTION(IS_NOT_DISTINCT_FROM)

#undef IS_DISTINCT_FROM
#undef IS_NOT_DISTINCT_FROM

#define DIVIDE(TYPE)                                                                     \
  FORCE_INLINE                                                                           \
  gdv_##TYPE divide_##TYPE##_##TYPE(gdv_int64 context, gdv_##TYPE in1, gdv_##TYPE in2) { \
    if (in2 == 0) {                                                                      \
      return static_cast<gdv_##TYPE>(NULL);                                              \
    }                                                                                    \
    return static_cast<gdv_##TYPE>(in1 / in2);                                           \
  }

NUMERIC_FUNCTION(DIVIDE)

#undef DIVIDE

#define DIV(TYPE)                                                                     \
  FORCE_INLINE                                                                        \
  gdv_##TYPE div_##TYPE##_##TYPE(gdv_int64 context, gdv_##TYPE in1, gdv_##TYPE in2) { \
    if (in2 == 0) {                                                                   \
      char const* err_msg = "divide by zero error";                                   \
      gdv_fn_context_set_error_msg(context, err_msg);                                 \
      return 0;                                                                       \
    }                                                                                 \
    return static_cast<gdv_##TYPE>(in1 / in2);                                        \
  }

DIV(int32)
DIV(int64)

#undef DIV

#define DIV_FLOAT(TYPE)                                                               \
  FORCE_INLINE                                                                        \
  gdv_##TYPE div_##TYPE##_##TYPE(gdv_int64 context, gdv_##TYPE in1, gdv_##TYPE in2) { \
    if (in2 == 0) {                                                                   \
      char const* err_msg = "divide by zero error";                                   \
      gdv_fn_context_set_error_msg(context, err_msg);                                 \
      return 0;                                                                       \
    }                                                                                 \
    return static_cast<gdv_##TYPE>(::trunc(in1 / in2));                               \
  }

DIV_FLOAT(float32)
DIV_FLOAT(float64)

#undef DIV_FLOAT

#define BITWISE_NOT(TYPE) \
  FORCE_INLINE            \
  gdv_##TYPE bitwise_not_##TYPE(gdv_##TYPE in) { return static_cast<gdv_##TYPE>(~in); }

BITWISE_NOT(int32)
BITWISE_NOT(int64)

#undef BITWISE_NOT
#define SHIFT_LEFT_INT(LTYPE, RTYPE)                                               \
  FORCE_INLINE                                                                      \
  gdv_##LTYPE shift_left_##LTYPE##_##RTYPE(gdv_##LTYPE in1, gdv_##RTYPE in2) {     \
    return static_cast<gdv_##LTYPE>(in1 << in2);                                    \
  }

SHIFT_LEFT_INT(int32, int32)
SHIFT_LEFT_INT(int64, int32)

#undef SHIFT_RIGHT_INT

#define SHIFT_RIGHT_INT(LTYPE, RTYPE)                                           \
  FORCE_INLINE                                                                  \
  gdv_##LTYPE shift_right_##LTYPE##_##RTYPE(gdv_##LTYPE in1, gdv_##RTYPE in2) { \
    return static_cast<gdv_##LTYPE>(in1 >> in2);                                \
  }

SHIFT_RIGHT_INT(int32, int32)
SHIFT_RIGHT_INT(int64, int32)

#undef SHIFT_RIGHT_INT

#undef DATE_FUNCTION
#undef DATE_TYPES
#undef NUMERIC_BOOL_DATE_TYPES
#undef NUMERIC_DATE_TYPES
#undef NUMERIC_FUNCTION
#undef NUMERIC_TYPES

}  // extern "C"
