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

#include <arrow/memory_pool.h>
#include <jni/dataset/concurrent_map.h>

#include "org_apache_arrow_dataset_jni_NativeMemoryPool.h"

static bool loaded = false;
static std::mutex load_mutex;
static jint JNI_VERSION = JNI_VERSION_1_6;

static jclass runtime_exception_class;

static jclass native_memory_reservation_class;
static jclass native_direct_memory_reservation_class;

static jmethodID reserve_memory_method;
static jmethodID unreserve_memory_method;

static jlong default_memory_pool_id = -1L;

class ReserveMemory : public arrow::ReservationListener {
 public:
  ReserveMemory(JavaVM* vm, jobject memory_reservation)
      : vm_(vm), memory_reservation_(memory_reservation) {}

  arrow::Status OnReservation(int64_t size) override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      return arrow::Status::Invalid("JNIEnv was not attached to current thread");
    }
    env->CallObjectMethod(memory_reservation_, reserve_memory_method, size);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      return arrow::Status::OutOfMemory("Memory reservation failed in Java");
    }
    return arrow::Status::OK();
  }

  arrow::Status OnRelease(int64_t size) override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      return arrow::Status::Invalid("JNIEnv was not attached to current thread");
    }
    env->CallObjectMethod(memory_reservation_, unreserve_memory_method, size);
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      return arrow::Status::Invalid("Memory unreservation failed in Java");
    }
    return arrow::Status::OK();
  }

  jobject GetMemoryReservation() {
    return memory_reservation_;
  }

 private:
  JavaVM* vm_;
  jobject memory_reservation_;
};


jclass MemoryPoolCreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

jmethodID MemoryPoolGetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
        " within signature" + std::string(sig);
    env->ThrowNew(runtime_exception_class, error_message.c_str());
  }
  return ret;
}

jmethodID MemoryPoolGetStaticMethodID(JNIEnv* env, jclass this_class,
                            const char* name, const char* sig) {
  jmethodID ret = env->GetStaticMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find static method " + std::string(name) +
        " within signature" + std::string(sig);
    env->ThrowNew(runtime_exception_class, error_message.c_str());
  }
  return ret;
}

jint memory_pool_onload(JavaVM* vm){
  std::lock_guard<std::mutex> lock_guard(load_mutex);
  if (loaded) {
    return JNI_VERSION;
  }

  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  runtime_exception_class =
      MemoryPoolCreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

  native_memory_reservation_class =
      MemoryPoolCreateGlobalClassReference(env,
                                 "Lorg/apache/arrow/"
                                 "memory/ReservationListener;");
  native_direct_memory_reservation_class =
      MemoryPoolCreateGlobalClassReference(env,
                                 "Lorg/apache/arrow/"
                                 "memory/DirectReservationListener;");

  reserve_memory_method =
      MemoryPoolGetMethodID(env, native_memory_reservation_class, "reserve", "(J)V");
  unreserve_memory_method =
      MemoryPoolGetMethodID(env, native_memory_reservation_class, "unreserve", "(J)V");
  default_memory_pool_id = reinterpret_cast<jlong>(arrow::default_memory_pool());
  env->ExceptionDescribe();
  loaded = true;

  return JNI_VERSION;
}

void memory_pool_on_unload(JavaVM* vm) {
  std::lock_guard<std::mutex> lock_guard(load_mutex);
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(native_memory_reservation_class);
  env->DeleteGlobalRef(native_direct_memory_reservation_class);
  default_memory_pool_id = -1L;
  loaded = false;
}

arrow::MemoryPool* lookup_memory_pool( jlong memory_pool_id) {
  return reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    getDefaultMemoryPool
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_NativeMemoryPool_getDefaultMemoryPool
    (JNIEnv *, jclass) {
  return default_memory_pool_id;
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    createListenableMemoryPool
 * Signature: (Lorg/apache/arrow/memory/ReservationListener;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_NativeMemoryPool_createListenableMemoryPool
    (JNIEnv* env, jclass, jobject jlistener) {
  jobject jlistener_ref = env->NewGlobalRef(jlistener);
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    env->ThrowNew(runtime_exception_class, "Unable to get JavaVM instance");
    return -1;
  }
  std::shared_ptr<arrow::ReservationListener> listener =
      std::make_shared<ReserveMemory>(vm, jlistener_ref);
  auto memory_pool =
      new arrow::ReservationListenableMemoryPool(arrow::default_memory_pool(), listener);
  return reinterpret_cast<jlong>(memory_pool);
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    releaseMemoryPool
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_NativeMemoryPool_releaseMemoryPool
    (JNIEnv* env, jclass, jlong memory_pool_id) {
  if (memory_pool_id == default_memory_pool_id) {
    return;
  }
  arrow::ReservationListenableMemoryPool* pool =
      reinterpret_cast<arrow::ReservationListenableMemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    return;
  }
  std::shared_ptr<ReserveMemory> rm =
      std::dynamic_pointer_cast<ReserveMemory>(pool->get_listener());
  if (rm == nullptr) {
    delete pool;
    return;
  }
  delete pool;
  env->DeleteGlobalRef(rm->GetMemoryReservation());
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    bytesAllocated
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_NativeMemoryPool_bytesAllocated
    (JNIEnv* env, jclass, jlong memory_pool_id){
  arrow::MemoryPool* pool =
      reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    env->ThrowNew(runtime_exception_class, "Memory pool instance not found");
    return -1;
  }
  return pool->bytes_allocated();
}
