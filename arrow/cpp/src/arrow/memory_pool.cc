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

#include "arrow/memory_pool.h"

#include <algorithm>  // IWYU pragma: keep
#include <cstdlib>    // IWYU pragma: keep
#include <cstring>    // IWYU pragma: keep
#include <iostream>   // IWYU pragma: keep
#include <limits>
#include <memory>
#include <mutex>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"  // IWYU pragma: keep
#include "arrow/util/optional.h"
#include "arrow/util/string.h"

#ifdef ARROW_JEMALLOC
// Needed to support jemalloc 3 and 4
#define JEMALLOC_MANGLE
// Explicitly link to our version of jemalloc
#include "jemalloc_ep/dist/include/jemalloc/jemalloc.h"
#endif

#ifdef ARROW_MIMALLOC
#include <mimalloc.h>
#endif

#ifdef ARROW_JEMALLOC

// Compile-time configuration for jemalloc options.
// Note the prefix ("je_arrow_") must match the symbol prefix given when
// building jemalloc.
// See discussion in https://github.com/jemalloc/jemalloc/issues/1621

// ARROW-6910(wesm): we found that jemalloc's default behavior with respect to
// dirty / muzzy pages (see definitions of these in the jemalloc documentation)
// conflicted with user expectations, and would even cause memory use problems
// in some cases. By enabling the background_thread option and reducing the
// decay time from 10 seconds to 1 seconds, memory is released more
// aggressively (and in the background) to the OS. This can be configured
// further by using the arrow::jemalloc_set_decay_ms API

#undef USE_JEMALLOC_BACKGROUND_THREAD
#ifndef __APPLE__
// ARROW-6977: jemalloc's background_thread isn't always enabled on macOS
#define USE_JEMALLOC_BACKGROUND_THREAD
#endif

// In debug mode, add memory poisoning on alloc / free
#ifdef NDEBUG
#define JEMALLOC_DEBUG_OPTIONS ""
#else
#define JEMALLOC_DEBUG_OPTIONS ",junk:true"
#endif

const char* je_arrow_malloc_conf =
    ("oversize_threshold:0"
#ifdef USE_JEMALLOC_BACKGROUND_THREAD
     ",dirty_decay_ms:1000"
     ",muzzy_decay_ms:1000"
     ",background_thread:true"
#else
     // ARROW-6994: return memory immediately to the OS if the
     // background_thread option isn't available
     ",dirty_decay_ms:0"
     ",muzzy_decay_ms:0"
#endif
     JEMALLOC_DEBUG_OPTIONS);  // NOLINT: whitespace/parens

#endif  // ARROW_JEMALLOC

namespace arrow {

namespace {

constexpr size_t kAlignment = 64;

constexpr char kDefaultBackendEnvVar[] = "ARROW_DEFAULT_MEMORY_POOL";

enum class MemoryPoolBackend : uint8_t { System, Jemalloc, Mimalloc };

struct SupportedBackend {
  const char* name;
  MemoryPoolBackend backend;
};

std::vector<SupportedBackend> SupportedBackends() {
  std::vector<SupportedBackend> backends = {
#ifdef ARROW_JEMALLOC
      {"jemalloc", MemoryPoolBackend::Jemalloc},
#endif
#ifdef ARROW_MIMALLOC
      {"mimalloc", MemoryPoolBackend::Mimalloc},
#endif
      {"system", MemoryPoolBackend::System}};
  return backends;
}

const std::vector<SupportedBackend> supported_backends = SupportedBackends();

util::optional<MemoryPoolBackend> UserSelectedBackend() {
  auto unsupported_backend = [](const std::string& name) {
    std::vector<std::string> supported;
    for (const auto backend : supported_backends) {
      supported.push_back(std::string("'") + backend.name + "'");
    }
    ARROW_LOG(WARNING) << "Unsupported backend '" << name << "' specified in "
                       << kDefaultBackendEnvVar << " (supported backends are "
                       << internal::JoinStrings(supported, ", ") << ")";
  };

  auto maybe_name = internal::GetEnvVar(kDefaultBackendEnvVar);
  if (!maybe_name.ok()) {
    return {};
  }
  const auto name = *std::move(maybe_name);
  if (name.empty()) {
    // An empty environment variable is considered missing
    return {};
  }
  const auto found =
      std::find_if(supported_backends.begin(), supported_backends.end(),
                   [&](const SupportedBackend& backend) { return name == backend.name; });
  if (found != supported_backends.end()) {
    return found->backend;
  }
  unsupported_backend(name);
  return {};
}

const util::optional<MemoryPoolBackend> user_selected_backend = UserSelectedBackend();

MemoryPoolBackend DefaultBackend() {
  auto backend = user_selected_backend;
  if (backend.has_value()) {
    return backend.value();
  }
#ifdef ARROW_JEMALLOC
  return MemoryPoolBackend::Jemalloc;
#elif defined(ARROW_MIMALLOC)
  return MemoryPoolBackend::Mimalloc;
#else
  return MemoryPoolBackend::System;
#endif
}

// A static piece of memory for 0-size allocations, so as to return
// an aligned non-null pointer.
alignas(kAlignment) static uint8_t zero_size_area[1];

// Helper class directing allocations to the standard system allocator.
class SystemAllocator {
 public:
  // Allocate memory according to the alignment requirements for Arrow
  // (as of May 2016 64 bytes)
  static Status AllocateAligned(int64_t size, uint8_t** out) {
    if (size == 0) {
      *out = zero_size_area;
      return Status::OK();
    }
#ifdef _WIN32
    // Special code path for Windows
    *out = reinterpret_cast<uint8_t*>(
        _aligned_malloc(static_cast<size_t>(size), kAlignment));
    if (!*out) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
#else
    const int result = posix_memalign(reinterpret_cast<void**>(out), kAlignment,
                                      static_cast<size_t>(size));
    if (result == ENOMEM) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }

    if (result == EINVAL) {
      return Status::Invalid("invalid alignment parameter: ", kAlignment);
    }
#endif
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size);
      *ptr = zero_size_area;
      return Status::OK();
    }
    // Note: We cannot use realloc() here as it doesn't guarantee alignment.

    // Allocate new chunk
    uint8_t* out = nullptr;
    RETURN_NOT_OK(AllocateAligned(new_size, &out));
    DCHECK(out);
    // Copy contents and release old memory chunk
    memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));
#ifdef _WIN32
    _aligned_free(*ptr);
#else
    free(*ptr);
#endif  // defined(_WIN32)
    *ptr = out;
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      DCHECK_EQ(size, 0);
    } else {
#ifdef _WIN32
      _aligned_free(ptr);
#else
      free(ptr);
#endif
    }
  }
};

#ifdef ARROW_JEMALLOC

// Helper class directing allocations to the jemalloc allocator.
class JemallocAllocator {
 public:
  static Status AllocateAligned(int64_t size, uint8_t** out) {
    if (size == 0) {
      *out = zero_size_area;
      return Status::OK();
    }
    *out = reinterpret_cast<uint8_t*>(
        mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(kAlignment)));
    if (*out == NULL) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size);
      *ptr = zero_size_area;
      return Status::OK();
    }
    *ptr = reinterpret_cast<uint8_t*>(
        rallocx(*ptr, static_cast<size_t>(new_size), MALLOCX_ALIGN(kAlignment)));
    if (*ptr == NULL) {
      *ptr = previous_ptr;
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      DCHECK_EQ(size, 0);
    } else {
      dallocx(ptr, MALLOCX_ALIGN(kAlignment));
    }
  }
};

#endif  // defined(ARROW_JEMALLOC)

#ifdef ARROW_MIMALLOC

// Helper class directing allocations to the mimalloc allocator.
class MimallocAllocator {
 public:
  static Status AllocateAligned(int64_t size, uint8_t** out) {
    if (size == 0) {
      *out = zero_size_area;
      return Status::OK();
    }
    *out = reinterpret_cast<uint8_t*>(
        mi_malloc_aligned(static_cast<size_t>(size), kAlignment));
    if (*out == NULL) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size);
      *ptr = zero_size_area;
      return Status::OK();
    }
    *ptr = reinterpret_cast<uint8_t*>(
        mi_realloc_aligned(previous_ptr, static_cast<size_t>(new_size), kAlignment));
    if (*ptr == NULL) {
      *ptr = previous_ptr;
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      DCHECK_EQ(size, 0);
    } else {
      mi_free(ptr);
    }
  }
};

#endif  // defined(ARROW_MIMALLOC)

}  // namespace

int64_t MemoryPool::max_memory() const { return -1; }

///////////////////////////////////////////////////////////////////////
// MemoryPool implementation that delegates its core duty
// to an Allocator class.

#ifndef NDEBUG
static constexpr uint8_t kAllocPoison = 0xBC;
static constexpr uint8_t kReallocPoison = 0xBD;
static constexpr uint8_t kDeallocPoison = 0xBE;
#endif

template <typename Allocator>
class BaseMemoryPoolImpl : public MemoryPool {
 public:
  ~BaseMemoryPoolImpl() override {}

  Status Allocate(int64_t size, uint8_t** out) override {
    if (size < 0) {
      return Status::Invalid("negative malloc size");
    }
    if (static_cast<uint64_t>(size) >= std::numeric_limits<size_t>::max()) {
      return Status::CapacityError("malloc size overflows size_t");
    }
    RETURN_NOT_OK(Allocator::AllocateAligned(size, out));
#ifndef NDEBUG
    // Poison data
    if (size > 0) {
      DCHECK_NE(*out, nullptr);
      (*out)[0] = kAllocPoison;
      (*out)[size - 1] = kAllocPoison;
    }
#endif

    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    if (new_size < 0) {
      return Status::Invalid("negative realloc size");
    }
    if (static_cast<uint64_t>(new_size) >= std::numeric_limits<size_t>::max()) {
      return Status::CapacityError("realloc overflows size_t");
    }
    RETURN_NOT_OK(Allocator::ReallocateAligned(old_size, new_size, ptr));
#ifndef NDEBUG
    // Poison data
    if (new_size > old_size) {
      DCHECK_NE(*ptr, nullptr);
      (*ptr)[old_size] = kReallocPoison;
      (*ptr)[new_size - 1] = kReallocPoison;
    }
#endif

    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override {
#ifndef NDEBUG
    // Poison data
    if (size > 0) {
      DCHECK_NE(buffer, nullptr);
      buffer[0] = kDeallocPoison;
      buffer[size - 1] = kDeallocPoison;
    }
#endif
    Allocator::DeallocateAligned(buffer, size);

    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

  int64_t max_memory() const override { return stats_.max_memory(); }

 protected:
  internal::MemoryPoolStats stats_;
};

class SystemMemoryPool : public BaseMemoryPoolImpl<SystemAllocator> {
 public:
  std::string backend_name() const override { return "system"; }
};

#ifdef ARROW_JEMALLOC
class JemallocMemoryPool : public BaseMemoryPoolImpl<JemallocAllocator> {
 public:
  std::string backend_name() const override { return "jemalloc"; }
};
#endif

#ifdef ARROW_MIMALLOC
class MimallocMemoryPool : public BaseMemoryPoolImpl<MimallocAllocator> {
 public:
  std::string backend_name() const override { return "mimalloc"; }
};
#endif

std::unique_ptr<MemoryPool> MemoryPool::CreateDefault() {
  auto backend = DefaultBackend();
  switch (backend) {
    case MemoryPoolBackend::System:
      return std::unique_ptr<MemoryPool>(new SystemMemoryPool);
#ifdef ARROW_JEMALLOC
    case MemoryPoolBackend::Jemalloc:
      return std::unique_ptr<MemoryPool>(new JemallocMemoryPool);
#endif
#ifdef ARROW_MIMALLOC
    case MemoryPoolBackend::Mimalloc:
      return std::unique_ptr<MemoryPool>(new MimallocMemoryPool);
#endif
    default:
      ARROW_LOG(FATAL) << "Internal error: cannot create default memory pool";
      return nullptr;
  }
}

static SystemMemoryPool system_pool;
#ifdef ARROW_JEMALLOC
static JemallocMemoryPool jemalloc_pool;
#endif
#ifdef ARROW_MIMALLOC
static MimallocMemoryPool mimalloc_pool;
#endif

MemoryPool* system_memory_pool() { return &system_pool; }

Status jemalloc_memory_pool(MemoryPool** out) {
#ifdef ARROW_JEMALLOC
  *out = &jemalloc_pool;
  return Status::OK();
#else
  return Status::NotImplemented("This Arrow build does not enable jemalloc");
#endif
}

Status mimalloc_memory_pool(MemoryPool** out) {
#ifdef ARROW_MIMALLOC
  *out = &mimalloc_pool;
  return Status::OK();
#else
  return Status::NotImplemented("This Arrow build does not enable mimalloc");
#endif
}

MemoryPool* default_memory_pool() {
  auto backend = DefaultBackend();
  switch (backend) {
    case MemoryPoolBackend::System:
      return &system_pool;
#ifdef ARROW_JEMALLOC
    case MemoryPoolBackend::Jemalloc:
      return &jemalloc_pool;
#endif
#ifdef ARROW_MIMALLOC
    case MemoryPoolBackend::Mimalloc:
      return &mimalloc_pool;
#endif
    default:
      ARROW_LOG(FATAL) << "Internal error: cannot create default memory pool";
      return nullptr;
  }
}

#define RETURN_IF_JEMALLOC_ERROR(ERR)                  \
  do {                                                 \
    if (err != 0) {                                    \
      return Status::UnknownError(std::strerror(ERR)); \
    }                                                  \
  } while (0)

Status jemalloc_set_decay_ms(int ms) {
#ifdef ARROW_JEMALLOC
  ssize_t decay_time_ms = static_cast<ssize_t>(ms);

  int err = mallctl("arenas.dirty_decay_ms", nullptr, nullptr, &decay_time_ms,
                    sizeof(decay_time_ms));
  RETURN_IF_JEMALLOC_ERROR(err);
  err = mallctl("arenas.muzzy_decay_ms", nullptr, nullptr, &decay_time_ms,
                sizeof(decay_time_ms));
  RETURN_IF_JEMALLOC_ERROR(err);

  return Status::OK();
#else
  return Status::Invalid("jemalloc support is not built");
#endif
}

///////////////////////////////////////////////////////////////////////
// LoggingMemoryPool implementation

LoggingMemoryPool::LoggingMemoryPool(MemoryPool* pool) : pool_(pool) {}

Status LoggingMemoryPool::Allocate(int64_t size, uint8_t** out) {
  Status s = pool_->Allocate(size, out);
  std::cout << "Allocate: size = " << size << std::endl;
  return s;
}

Status LoggingMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  Status s = pool_->Reallocate(old_size, new_size, ptr);
  std::cout << "Reallocate: old_size = " << old_size << " - new_size = " << new_size
            << std::endl;
  return s;
}

void LoggingMemoryPool::Free(uint8_t* buffer, int64_t size) {
  pool_->Free(buffer, size);
  std::cout << "Free: size = " << size << std::endl;
}

int64_t LoggingMemoryPool::bytes_allocated() const {
  int64_t nb_bytes = pool_->bytes_allocated();
  std::cout << "bytes_allocated: " << nb_bytes << std::endl;
  return nb_bytes;
}

int64_t LoggingMemoryPool::max_memory() const {
  int64_t mem = pool_->max_memory();
  std::cout << "max_memory: " << mem << std::endl;
  return mem;
}

std::string LoggingMemoryPool::backend_name() const { return pool_->backend_name(); }

///////////////////////////////////////////////////////////////////////
// ProxyMemoryPool implementation

class ProxyMemoryPool::ProxyMemoryPoolImpl {
 public:
  explicit ProxyMemoryPoolImpl(MemoryPool* pool) : pool_(pool) {}

  Status Allocate(int64_t size, uint8_t** out) {
    RETURN_NOT_OK(pool_->Allocate(size, out));
    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    RETURN_NOT_OK(pool_->Reallocate(old_size, new_size, ptr));
    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) {
    pool_->Free(buffer, size);
    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t bytes_allocated() const { return stats_.bytes_allocated(); }

  int64_t max_memory() const { return stats_.max_memory(); }

  std::string backend_name() const { return pool_->backend_name(); }

 private:
  MemoryPool* pool_;
  internal::MemoryPoolStats stats_;
};

ProxyMemoryPool::ProxyMemoryPool(MemoryPool* pool) {
  impl_.reset(new ProxyMemoryPoolImpl(pool));
}

ProxyMemoryPool::~ProxyMemoryPool() {}

Status ProxyMemoryPool::Allocate(int64_t size, uint8_t** out) {
  return impl_->Allocate(size, out);
}

Status ProxyMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  return impl_->Reallocate(old_size, new_size, ptr);
}

void ProxyMemoryPool::Free(uint8_t* buffer, int64_t size) {
  return impl_->Free(buffer, size);
}

int64_t ProxyMemoryPool::bytes_allocated() const { return impl_->bytes_allocated(); }

int64_t ProxyMemoryPool::max_memory() const { return impl_->max_memory(); }

std::string ProxyMemoryPool::backend_name() const { return impl_->backend_name(); }

std::vector<std::string> SupportedMemoryBackendNames() {
  std::vector<std::string> supported;
  for (const auto backend : supported_backends) {
    supported.push_back(backend.name);
  }
  return supported;
}

ReservationListener::~ReservationListener() {}

ReservationListener::ReservationListener() {}

class ReservationListenableMemoryPool::ReservationListenableMemoryPoolImpl {
 public:
  explicit ReservationListenableMemoryPoolImpl(
      MemoryPool* pool, std::shared_ptr<ReservationListener> listener, int64_t block_size)
      : pool_(pool),
        listener_(listener),
        block_size_(block_size),
        blocks_reserved_(0),
        bytes_reserved_(0) {}

  Status Allocate(int64_t size, uint8_t** out) {
    RETURN_NOT_OK(UpdateReservation(size));
    Status error = pool_->Allocate(size, out);
    if (!error.ok()) {
      RETURN_NOT_OK(UpdateReservation(-size));
      return error;
    }
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    bool reserved = false;
    int64_t diff = new_size - old_size;
    if (new_size >= old_size) {
      RETURN_NOT_OK(UpdateReservation(diff));
      reserved = true;
    }
    Status error = pool_->Reallocate(old_size, new_size, ptr);
    if (!error.ok()) {
      if (reserved) {
        RETURN_NOT_OK(UpdateReservation(-diff));
      }
      return error;
    }
    if (!reserved) {
      RETURN_NOT_OK(UpdateReservation(diff));
    }
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) {
    pool_->Free(buffer, size);
    // fixme currently method ::Free doesn't allow Status return
    Status s = UpdateReservation(-size);
    if (!s.ok()) {
      ARROW_LOG(FATAL) << "Failed to update reservation while freeing bytes: "
                       << s.message();
      return;
    }
  }

  Status UpdateReservation(int64_t diff) {
    int64_t granted = Reserve(diff);
    if (granted == 0) {
      return Status::OK();
    }
    if (granted < 0) {
      RETURN_NOT_OK(listener_->OnRelease(-granted));
      return Status::OK();
    }
    RETURN_NOT_OK(listener_->OnReservation(granted));
    return Status::OK();
  }

  int64_t Reserve(int64_t diff) {
    std::lock_guard<std::mutex> lock(mutex_);
    bytes_reserved_ += diff;
    int64_t new_block_count;
    if (bytes_reserved_ == 0) {
      new_block_count = 0;
    } else {
      new_block_count = (bytes_reserved_ - 1) / block_size_ + 1;
    }
    int64_t bytes_granted = (new_block_count - blocks_reserved_) * block_size_;
    blocks_reserved_ = new_block_count;
    return bytes_granted;
  }

  int64_t bytes_allocated() {
    std::lock_guard<std::mutex> lock(mutex_);
    return bytes_reserved_;
  }

  int64_t max_memory() { return pool_->max_memory(); }

  std::string backend_name() { return pool_->backend_name(); }

  std::shared_ptr<ReservationListener> get_listener() { return listener_; }

 private:
  MemoryPool* pool_;
  std::shared_ptr<ReservationListener> listener_;
  int64_t block_size_;
  int64_t blocks_reserved_;
  int64_t bytes_reserved_;
  std::mutex mutex_;
};

ReservationListenableMemoryPool::~ReservationListenableMemoryPool() {}

ReservationListenableMemoryPool::ReservationListenableMemoryPool(
    MemoryPool* pool, std::shared_ptr<ReservationListener> listener, int64_t block_size) {
  impl_.reset(new ReservationListenableMemoryPoolImpl(pool, listener, block_size));
}

Status ReservationListenableMemoryPool::Allocate(int64_t size, uint8_t** out) {
  return impl_->Allocate(size, out);
}

Status ReservationListenableMemoryPool::Reallocate(int64_t old_size, int64_t new_size,
                                                   uint8_t** ptr) {
  return impl_->Reallocate(old_size, new_size, ptr);
}

void ReservationListenableMemoryPool::Free(uint8_t* buffer, int64_t size) {
  return impl_->Free(buffer, size);
}

int64_t ReservationListenableMemoryPool::bytes_allocated() const {
  return impl_->bytes_allocated();
}

int64_t ReservationListenableMemoryPool::max_memory() const {
  return impl_->max_memory();
}

std::string ReservationListenableMemoryPool::backend_name() const {
  return impl_->backend_name();
}

std::shared_ptr<ReservationListener> ReservationListenableMemoryPool::get_listener() {
  return impl_->get_listener();
}

}  // namespace arrow
