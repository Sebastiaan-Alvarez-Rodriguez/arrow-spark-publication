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

#include <arrow/array.h>
#include <arrow/compare.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/kernel.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/expression.h>
#include <arrow/dataset/file_base.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/iterator.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/message.h>
#include <jni/dataset/DTypes.pb.h>
#include <jni/dataset/concurrent_map.h>
#include <jni/dataset/jni_memory_pool.h>

#include <mutex>

#include "org_apache_arrow_dataset_file_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_JniWrapper.h"

static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;
static jclass runtime_exception_class;

static jclass record_batch_handle_class;
static jclass record_batch_handle_field_class;
static jclass record_batch_handle_buffer_class;
static jclass dictionary_batch_handle_class;

static jmethodID record_batch_handle_constructor;
static jmethodID record_batch_handle_field_constructor;
static jmethodID record_batch_handle_buffer_constructor;
static jmethodID dictionary_batch_handle_constructor;

static jint JNI_VERSION = JNI_VERSION_1_6;

using arrow::jni::ConcurrentMap;

static ConcurrentMap<std::shared_ptr<arrow::dataset::DatasetFactory>> dataset_factory_holder_;
static ConcurrentMap<std::shared_ptr<arrow::dataset::Dataset>> dataset_holder_;
static ConcurrentMap<std::shared_ptr<arrow::dataset::ScanTask>> scan_task_holder_;
static ConcurrentMap<std::shared_ptr<arrow::dataset::Scanner>> scanner_holder_;
static ConcurrentMap<std::shared_ptr<arrow::RecordBatchIterator>> iterator_holder_;
static ConcurrentMap<std::shared_ptr<arrow::Buffer>> buffer_holder_;

#define JNI_ASSIGN_OR_THROW_NAME(x, y) ARROW_CONCAT(x, y)

#define JNI_ASSIGN_OR_THROW_IMPL(t, lhs, rexpr, fallback)                 \
  auto t = (rexpr);                                                       \
  if (!t.status().ok()) {                                                 \
    env->ThrowNew(runtime_exception_class, t.status().message().c_str()); \
  }                                                                       \
  lhs = std::move(t).ValueOr(fallback);

#define JNI_ASSIGN_OR_THROW(lhs, rexpr) \
  JNI_ASSIGN_OR_THROW_IMPL(JNI_ASSIGN_OR_THROW_NAME(_tmp_var, __COUNTER__), lhs, rexpr, \
      nullptr)

#define JNI_ASSIGN_OR_THROW_WITH_FALLBACK(lhs, rexpr, fallback) \
  JNI_ASSIGN_OR_THROW_IMPL(JNI_ASSIGN_OR_THROW_NAME(_tmp_var, __COUNTER__), lhs, rexpr, \
      fallback)

#define JNI_ASSERT_OK_OR_THROW(expr)                                          \
  do {                                                                        \
    auto _res = (expr);                                                       \
    arrow::Status _st = ::arrow::internal::GenericToStatus(_res);             \
    if (!_st.ok()) {                                                          \
       env->ThrowNew(runtime_exception_class, _st.message().c_str());  \
    }                                                                         \
  } while (false);

#define FORMAT_PARQUET 0
#define FORMAT_CSV 1

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
        " within signature" + std::string(sig);
    env->ThrowNew(illegal_access_exception_class, error_message.c_str());
  }
  return ret;
}

jmethodID GetStaticMethodID(JNIEnv* env, jclass this_class,
                                           const char* name, const char* sig) {
  jmethodID ret = env->GetStaticMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find static method " + std::string(name) +
        " within signature" + std::string(sig);
    env->ThrowNew(illegal_access_exception_class, error_message.c_str());
  }
  return ret;
}

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  runtime_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

  record_batch_handle_class = CreateGlobalClassReference(
      env, "Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle;");
  record_batch_handle_field_class = CreateGlobalClassReference(
      env, "Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle$Field;");
  record_batch_handle_buffer_class = CreateGlobalClassReference(
      env, "Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle$Buffer;");


  record_batch_handle_constructor =
      GetMethodID(env, record_batch_handle_class, "<init>",
                  "(J[Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle$Field;"
                  "[Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle$Buffer;)V");

  dictionary_batch_handle_class = CreateGlobalClassReference(
      env, "Lorg/apache/arrow/dataset/jni/NativeDictionaryBatchHandle;");

  dictionary_batch_handle_constructor =
      GetMethodID(env, dictionary_batch_handle_class, "<init>",
                  "(JJ[Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle$Field;"
                  "[Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle$Buffer;)V");

  record_batch_handle_field_constructor =
      GetMethodID(env, record_batch_handle_field_class, "<init>", "(JJ)V");
  record_batch_handle_buffer_constructor =
      GetMethodID(env, record_batch_handle_buffer_class, "<init>", "(JJJJ)V");
  memory_pool_onload(vm);
  env->ExceptionDescribe();

  return JNI_VERSION;
}


void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);
  env->DeleteGlobalRef(runtime_exception_class);
  env->DeleteGlobalRef(record_batch_handle_class);
  env->DeleteGlobalRef(record_batch_handle_field_class);
  env->DeleteGlobalRef(record_batch_handle_buffer_class);
  env->DeleteGlobalRef(dictionary_batch_handle_class);

  dataset_factory_holder_.Clear();
  dataset_holder_.Clear();
  scan_task_holder_.Clear();
  scanner_holder_.Clear();
  iterator_holder_.Clear();
  buffer_holder_.Clear();

  memory_pool_on_unload(vm);
}

std::shared_ptr<arrow::Schema> SchemaFromColumnNames(
    const std::shared_ptr<arrow::Schema>& input, const std::vector<std::string>& column_names) {
  std::vector<std::shared_ptr<arrow::Field>> columns;
  for (const auto& name : column_names) {
    columns.push_back(input->GetFieldByName(name));
  }
  return std::make_shared<arrow::Schema>(columns);
}

std::shared_ptr<arrow::dataset::FileFormat> GetFileFormat(JNIEnv *env, jint id) {
  switch (id) {
    case FORMAT_PARQUET:
      return std::make_shared<arrow::dataset::ParquetFileFormat>();
    case FORMAT_CSV:
      return std::make_shared<arrow::dataset::CsvFileFormat>();
    default:
      std::string error_message = "illegal file format id: " + std::to_string(id);
      env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
      return nullptr;  // unreachable
  }
}

std::string JStringToCString(JNIEnv* env, jstring string) {
  jboolean copied;
  int32_t length = env->GetStringUTFLength(string);
  const char *chars = env->GetStringUTFChars(string, &copied);
  std::string str = std::string(chars, length);
  // fixme calling ReleaseStringUTFChars if memory leak faced
  return str;
}

std::vector<std::string> ToStringVector(JNIEnv* env, jobjectArray& str_array) {
  int length = env->GetArrayLength(str_array);
  std::vector<std::string> vector;
  for (int i = 0; i < length; i++) {
    auto string = (jstring) (env->GetObjectArrayElement(str_array, i));
    vector.push_back(JStringToCString(env, string));
  }
  return vector;
}

template <typename T>
std::vector<T> collect(JNIEnv* env, arrow::Iterator<T> itr) {
  std::vector<T> vector;
  while(true) {
    JNI_ASSIGN_OR_THROW(T t, itr.Next())
    if (!t) {
      break;
    }
    vector.push_back(t);
  }
  return vector;
}

jobjectArray makeObjectArray(JNIEnv* env, jclass clazz, std::vector<jobject> args) {
  jobjectArray oa = env->NewObjectArray(args.size(), clazz, 0);
  for (size_t i = 0; i < args.size(); i++) {
    env->SetObjectArrayElement(oa, i, args.at(i));
  }
  return oa;
}

// FIXME: COPIED FROM intel/master on which this branch is not rebased yet
// FIXME: https://github.com/Intel-bigdata/arrow/blob/02502a4eb59834c2471dd629e77dbeed19559f68/cpp/src/jni/jni_common.h#L239-L254
jbyteArray ToSchemaByteArray(JNIEnv* env, std::shared_ptr<arrow::Schema> schema) {
  JNI_ASSIGN_OR_THROW(std::shared_ptr<arrow::Buffer> buffer,
      arrow::ipc::SerializeSchema(*schema.get(), arrow::default_memory_pool()));

  jbyteArray out = env->NewByteArray(buffer->size());
  auto src = reinterpret_cast<const jbyte*>(buffer->data());
  env->SetByteArrayRegion(out, 0, buffer->size(), src);
  return out;
}

// FIXME: COPIED FROM intel/master on which this branch is not rebased yet
// FIXME: https://github.com/Intel-bigdata/arrow/blob/02502a4eb59834c2471dd629e77dbeed19559f68/cpp/src/jni/jni_common.h#L256-L272
std::shared_ptr<arrow::Schema> FromSchemaByteArray(JNIEnv* env, jbyteArray schemaBytes) {
  arrow::ipc::DictionaryMemo in_memo;
  int schemaBytes_len = env->GetArrayLength(schemaBytes);
  jbyte* schemaBytes_data = env->GetByteArrayElements(schemaBytes, 0);
  auto serialized_schema =
      std::make_shared<arrow::Buffer>((uint8_t*)schemaBytes_data, schemaBytes_len);
  arrow::io::BufferReader buf_reader(serialized_schema);
  JNI_ASSIGN_OR_THROW(std::shared_ptr<arrow::Schema> schema, arrow::ipc::ReadSchema(&buf_reader, &in_memo))
  env->ReleaseByteArrayElements(schemaBytes, schemaBytes_data, JNI_ABORT);
  return schema;
}

bool ParseProtobuf(uint8_t* buf, int bufLen, google::protobuf::Message* msg) {
    google::protobuf::io::CodedInputStream cis(buf, bufLen);
    cis.SetRecursionLimit(1000);
    return msg->ParseFromCodedStream(&cis);
}

void releaseFilterInput(jbyteArray condition_arr, jbyte* condition_bytes, JNIEnv* env) {
  env->ReleaseByteArrayElements(condition_arr, condition_bytes, JNI_ABORT);
}

// fixme in development. Not all node types considered.
arrow::dataset::Expression translateNode(arrow::dataset::types::TreeNode node, JNIEnv* env) {
  if (node.has_fieldnode()) {
    const arrow::dataset::types::FieldNode& f_node = node.fieldnode();
    const std::string& name = f_node.name();
    return arrow::dataset::field_ref(name);
  }
  if (node.has_intnode()) {
    const arrow::dataset::types::IntNode& int_node = node.intnode();
    int32_t val = int_node.value();
    return arrow::dataset::literal(val);
  }
  if (node.has_longnode()) {
    const arrow::dataset::types::LongNode& long_node = node.longnode();
    int64_t val = long_node.value();
    return arrow::dataset::literal(val);
  }
  if (node.has_floatnode()) {
    const arrow::dataset::types::FloatNode& float_node = node.floatnode();
    float val = float_node.value();
    return arrow::dataset::literal(val);
  }
  if (node.has_doublenode()) {
    const arrow::dataset::types::DoubleNode& double_node = node.doublenode();
    double val = double_node.value();
    return arrow::dataset::literal(std::make_shared<arrow::DoubleScalar>(val));
  }
  if (node.has_booleannode()) {
    const arrow::dataset::types::BooleanNode& boolean_node = node.booleannode();
    bool val = boolean_node.value();
    return arrow::dataset::literal(val);
  }
  if (node.has_andnode()) {
    const arrow::dataset::types::AndNode& and_node = node.andnode();
    const arrow::dataset::types::TreeNode& left_arg = and_node.leftarg();
    const arrow::dataset::types::TreeNode& right_arg = and_node.rightarg();
    return arrow::dataset::and_(translateNode(left_arg, env), translateNode(right_arg, env));
  }
  if (node.has_ornode()) {
    const arrow::dataset::types::OrNode& or_node = node.ornode();
    const arrow::dataset::types::TreeNode& left_arg = or_node.leftarg();
    const arrow::dataset::types::TreeNode& right_arg = or_node.rightarg();
    return arrow::dataset::or_(translateNode(left_arg, env), translateNode(right_arg, env));
  }
  if (node.has_cpnode()) {
    const arrow::dataset::types::ComparisonNode& cp_node = node.cpnode();
    const std::string& op_name = cp_node.opname();
    const arrow::dataset::types::TreeNode& left_arg = cp_node.leftarg();
    const arrow::dataset::types::TreeNode& right_arg = cp_node.rightarg();
    return arrow::dataset::call(op_name,
        {translateNode(left_arg, env), translateNode(right_arg, env)});
  }
  if (node.has_notnode()) {
    const arrow::dataset::types::NotNode& not_node = node.notnode();
    const ::arrow::dataset::types::TreeNode& child = not_node.args();
    arrow::dataset::Expression translatedChild = translateNode(child, env);
    return arrow::dataset::not_(translatedChild);
  }
  if (node.has_isvalidnode()) {
    const arrow::dataset::types::IsValidNode& is_valid_node = node.isvalidnode();
    const ::arrow::dataset::types::TreeNode& child = is_valid_node.args();
    arrow::dataset::Expression translatedChild = translateNode(child, env);
    return arrow::dataset::call("is_valid", {translatedChild});
  }
  std::string error_message = "Unknown node type";
  env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  return arrow::dataset::literal(false); // unreachable
}

arrow::dataset::Expression translateFilter(arrow::dataset::types::Condition condition, JNIEnv* env) {
  const arrow::dataset::types::TreeNode& tree_node = condition.root();
  return translateNode(tree_node, env);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDatasetFactory
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDatasetFactory
    (JNIEnv *, jobject, jlong id) {
  dataset_factory_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    inspectSchema
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_inspectSchema
    (JNIEnv* env, jobject, jlong dataset_factor_id) {
  std::shared_ptr<arrow::dataset::DatasetFactory> d
      = dataset_factory_holder_.Lookup(dataset_factor_id);
  JNI_ASSIGN_OR_THROW(std::shared_ptr<arrow::Schema> schema, d->Inspect())
  return ToSchemaByteArray(env, schema);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    createDataset
 * Signature: (J[B)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createDataset
    (JNIEnv* env, jobject, jlong dataset_factory_id, jbyteArray schema_bytes) {
  std::shared_ptr<arrow::dataset::DatasetFactory> d
      = dataset_factory_holder_.Lookup(dataset_factory_id);
  std::shared_ptr<arrow::Schema> schema;
  schema = FromSchemaByteArray(env, schema_bytes);
  JNI_ASSIGN_OR_THROW(std::shared_ptr<arrow::dataset::Dataset> dataset, d->Finish(schema))
  return dataset_holder_.Insert(dataset);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDataset
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDataset
    (JNIEnv *, jobject, jlong id) {
  dataset_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    createScanner
 * Signature: (J[Ljava/lang/String;[BJJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createScanner(
    JNIEnv* env, jobject, jlong dataset_id, jobjectArray columns, jbyteArray filter,
    jlong batch_size, jlong memory_pool_id) {
  std::shared_ptr<arrow::dataset::ScanContext> context =
      std::make_shared<arrow::dataset::ScanContext>();
  arrow::MemoryPool* pool = lookup_memory_pool(memory_pool_id);
  if (pool == nullptr) {
    env->ThrowNew(runtime_exception_class,
        "Memory pool does not exist or has been closed");
    return -1;
  }
  context->pool = pool;
  std::shared_ptr<arrow::dataset::Dataset> dataset = dataset_holder_.Lookup(dataset_id);
  JNI_ASSIGN_OR_THROW(std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder,
                      dataset->NewScan(context))

  std::vector<std::string> column_vector = ToStringVector(env, columns);
  JNI_ASSERT_OK_OR_THROW(scanner_builder->Project(column_vector));
  JNI_ASSERT_OK_OR_THROW(scanner_builder->BatchSize(batch_size));

  // initialize filters
  jsize exprs_len = env->GetArrayLength(filter);
  jbyte* exprs_bytes = env->GetByteArrayElements(filter, 0);
  arrow::dataset::types::Condition condition;
  if (!ParseProtobuf(reinterpret_cast<uint8_t*>(exprs_bytes), exprs_len, &condition)) {
    releaseFilterInput(filter, exprs_bytes, env);
    std::string error_message = "bad protobuf message";
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  if (condition.has_root()) {
    JNI_ASSERT_OK_OR_THROW(scanner_builder->Filter(translateFilter(condition, env)));
  }
  JNI_ASSIGN_OR_THROW(auto scanner, scanner_builder->Finish())
  jlong id = scanner_holder_.Insert(scanner);
  releaseFilterInput(filter, exprs_bytes, env);
  return id;
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    getSchemaFromScanner
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_getSchemaFromScanner
    (JNIEnv* env, jobject, jlong scanner_id) {
  std::shared_ptr<arrow::Schema> schema = scanner_holder_.Lookup(scanner_id)->schema();
  return ToSchemaByteArray(env, schema);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    getScanTasksFromScanner
 * Signature: (J)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_getScanTasksFromScanner
    (JNIEnv* env, jobject, jlong scanner_id) {
  std::shared_ptr<arrow::dataset::Scanner> scanner = scanner_holder_.Lookup(scanner_id);
  JNI_ASSIGN_OR_THROW_WITH_FALLBACK(arrow::dataset::ScanTaskIterator itr,
      scanner->ScanWithWeakFilter(),
      arrow::MakeEmptyIterator<std::shared_ptr<arrow::dataset::ScanTask>>())
  std::vector<std::shared_ptr<arrow::dataset::ScanTask>> vector =
      collect(env, std::move(itr));
  jlongArray ret = env->NewLongArray(vector.size());
  for (size_t i = 0; i < vector.size(); i++) {
    std::shared_ptr<arrow::dataset::ScanTask> scan_task = vector.at(i);
    jlong id[] = {scan_task_holder_.Insert(scan_task)};
    env->SetLongArrayRegion(ret, i, 1, id);
  }
  return ret;
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeScanner
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeScanner
    (JNIEnv *, jobject, jlong scanner_id) {
  scanner_holder_.Erase(scanner_id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeScanTask
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeScanTask
    (JNIEnv *, jobject, jlong id) {
  scan_task_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    scan
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_scan(
    JNIEnv* env, jobject, jlong scan_task_id) {
  std::shared_ptr<arrow::dataset::ScanTask> scan_task =
      scan_task_holder_.Lookup(scan_task_id);
  JNI_ASSIGN_OR_THROW_WITH_FALLBACK(arrow::RecordBatchIterator record_batch_iterator,
      scan_task->Execute(),
      arrow::MakeEmptyIterator<std::shared_ptr<arrow::RecordBatch>>())
  return iterator_holder_.Insert(std::make_shared<arrow::RecordBatchIterator>(
      std::move(record_batch_iterator)));  // move and propagate
}


template<typename HandleCreator>
jobject createJavaHandle(JNIEnv *env,
    std::shared_ptr<arrow::RecordBatch> &record_batch,
    HandleCreator handle_creator) {
  std::shared_ptr<arrow::Schema> schema = record_batch->schema();
  jobjectArray field_array =
      env->NewObjectArray(schema->num_fields(), record_batch_handle_field_class, nullptr);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto column = record_batch->column(i);
    auto dataArray = column->data();
    jobject field = env->NewObject(record_batch_handle_field_class, record_batch_handle_field_constructor,
                                   column->length(), column->null_count());
    env->SetObjectArrayElement(field_array, i, field);

    for (auto& buffer : dataArray->buffers) {
      buffers.push_back(buffer);
    }
  }

  jobjectArray buffer_array =
      env->NewObjectArray(buffers.size(), record_batch_handle_buffer_class, nullptr);

  for (size_t j = 0; j < buffers.size(); ++j) {
    auto buffer = buffers[j];
    uint8_t* data = nullptr;
    int64_t size = 0;
    int64_t capacity = 0;
    if (buffer != nullptr) {
      data = (uint8_t*) buffer->data();
      size = buffer->size();
      capacity = buffer->capacity();
    }
    jobject buffer_handle = env->NewObject(record_batch_handle_buffer_class, record_batch_handle_buffer_constructor,
                                           buffer_holder_.Insert(buffer), data,
                                           size, capacity);
    env->SetObjectArrayElement(buffer_array, j, buffer_handle);
  }
  int64_t num_rows = record_batch->num_rows();
  jobject ret = handle_creator(num_rows, field_array, buffer_array);
  return ret;
}

jobject createJavaRecordBatchHandle(JNIEnv *env,
    std::shared_ptr<arrow::RecordBatch> &record_batch) {
  auto handle_creator = [env] (int64_t num_rows, jobjectArray field_array,
      jobjectArray buffer_array) {
    return env->NewObject(record_batch_handle_class, record_batch_handle_constructor,
        num_rows, field_array, buffer_array);
  };
  return createJavaHandle(env, record_batch, handle_creator);
}

jobject createJavaDictionaryBatchHandle(JNIEnv *env, jlong id,
    std::shared_ptr<arrow::RecordBatch> &record_batch) {
  auto handle_creator = [env, id] (int64_t num_rows, jobjectArray field_array,
                               jobjectArray buffer_array) {
    return env->NewObject(dictionary_batch_handle_class,
        dictionary_batch_handle_constructor, id, num_rows, field_array, buffer_array);
  };
  return createJavaHandle(env, record_batch, handle_creator);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    nextRecordBatch
 * Signature: (J)[Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle;
 */
jobjectArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_nextRecordBatch(
    JNIEnv* env, jobject, jlong iterator_id) {
  std::shared_ptr<arrow::RecordBatchIterator> itr = iterator_holder_.Lookup(iterator_id);

  JNI_ASSIGN_OR_THROW(std::shared_ptr<arrow::RecordBatch> record_batch, itr->Next())
  if (record_batch == nullptr) {
    return nullptr;  // stream ended
  }
  std::vector<jobject> handles;
  jobject handle = createJavaRecordBatchHandle(env, record_batch);
  handles.push_back(handle);

  // dictionary batches
  int num_columns = record_batch->num_columns();
  long dict_id = 0;
  for (int i = 0; i < num_columns; i++) {
    // defer to Java dictionary batch rule: a single array per batch
    std::shared_ptr<arrow::Field> field = record_batch->schema()->field(i);
    std::shared_ptr<arrow::Array> data = record_batch->column(i);
    std::shared_ptr<arrow::DataType> type = field->type();
    if (type->id() == arrow::Type::DICTIONARY) {
      std::shared_ptr<arrow::DataType> value_type =
          arrow::internal::checked_cast<const arrow::DictionaryType &>(*type)
              .value_type();
      std::shared_ptr<arrow::DictionaryArray> dict_data =
          arrow::internal::checked_pointer_cast<arrow::DictionaryArray>(data);
      std::shared_ptr<arrow::Field>
          value_field = std::make_shared<arrow::Field>(field->name(), value_type);
      std::vector<std::shared_ptr<arrow::Field>> dict_batch_fields;
      dict_batch_fields.push_back(value_field);
      std::shared_ptr<arrow::Schema>
          dict_batch_schema = std::make_shared<arrow::Schema>(dict_batch_fields);
      std::vector<std::shared_ptr<arrow::Array>> dict_datum;
      dict_datum.push_back(dict_data->dictionary());
      std::shared_ptr<arrow::RecordBatch>
          dict_batch = arrow::RecordBatch::Make(dict_batch_schema, dict_data->length(),
              dict_datum);
      jobject dict_handle = createJavaDictionaryBatchHandle(env, dict_id++, dict_batch);
      handles.push_back(dict_handle);
    }
  }
  return makeObjectArray(env, record_batch_handle_class, handles);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeIterator
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeIterator
    (JNIEnv *, jobject, jlong id) {
  iterator_holder_.Erase(id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    releaseBuffer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_releaseBuffer
    (JNIEnv *, jobject, jlong id) {
  buffer_holder_.Erase(id);
}


/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    makeSingleFileDatasetFactory
 * Signature: (Ljava/lang/String;II)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_makeSingleFileDatasetFactory(
    JNIEnv* env, jobject, jstring uri, jint file_format_id,
    jlong start_offset, jlong length) {
  std::shared_ptr<arrow::dataset::FileFormat> file_format =
      GetFileFormat(env, file_format_id);
  arrow::dataset::FileSystemFactoryOptions options;
  JNI_ASSIGN_OR_THROW(std::shared_ptr<arrow::dataset::DatasetFactory> d,
      arrow::dataset::FileSystemDatasetFactory::Make(
          JStringToCString(env, uri), start_offset, length, file_format, options))
  return dataset_factory_holder_.Insert(d);
}