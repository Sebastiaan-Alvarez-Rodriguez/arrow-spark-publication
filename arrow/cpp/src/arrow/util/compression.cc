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

#include "arrow/util/compression.h"

#include <memory>
#include <string>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/compression_internal.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace util {

int Codec::UseDefaultCompressionLevel() { return kUseDefaultCompressionLevel; }

Status Codec::Init() { return Status::OK(); }

const std::string& Codec::GetCodecAsString(Compression::type t) {
  static const std::string uncompressed = "uncompressed", snappy = "snappy",
                           gzip = "gzip", lzo = "lzo", brotli = "brotli",
                           lz4_raw = "lz4_raw", lz4 = "lz4", lz4_hadoop = "lz4_hadoop",
                           zstd = "zstd", bz2 = "bz2", fastpfor = "fastpfor", unknown = "unknown";

  switch (t) {
    case Compression::UNCOMPRESSED:
      return uncompressed;
    case Compression::SNAPPY:
      return snappy;
    case Compression::GZIP:
      return gzip;
    case Compression::LZO:
      return lzo;
    case Compression::BROTLI:
      return brotli;
    case Compression::LZ4:
      return lz4_raw;
    case Compression::LZ4_FRAME:
      return lz4;
    case Compression::LZ4_HADOOP:
      return lz4_hadoop;
    case Compression::ZSTD:
      return zstd;
    case Compression::BZ2:
      return bz2;
    case Compression::FASTPFOR:
      return fastpfor;
    default:
      return unknown;
  }
}

Result<Compression::type> Codec::GetCompressionType(const std::string& name) {
  if (name == "uncompressed") {
    return Compression::UNCOMPRESSED;
  } else if (name == "gzip") {
    return Compression::GZIP;
  } else if (name == "snappy") {
    return Compression::SNAPPY;
  } else if (name == "lzo") {
    return Compression::LZO;
  } else if (name == "brotli") {
    return Compression::BROTLI;
  } else if (name == "lz4_raw") {
    return Compression::LZ4;
  } else if (name == "lz4") {
    return Compression::LZ4_FRAME;
  } else if (name == "lz4_hadoop") {
    return Compression::LZ4_HADOOP;
  } else if (name == "zstd") {
    return Compression::ZSTD;
  } else if (name == "bz2") {
    return Compression::BZ2;
  } else if (name == "FASTPFOR") {
    return Compression::FASTPFOR;
  } else {
    return Status::Invalid("Unrecognized compression type: ", name);
  }
}

bool Codec::SupportsCompressionLevel(Compression::type codec) {
  switch (codec) {
    case Compression::GZIP:
    case Compression::BROTLI:
    case Compression::ZSTD:
    case Compression::BZ2:
      return true;
    default:
      return false;
  }
}

Result<std::unique_ptr<Codec>> Codec::Create(Compression::type codec_type,
                                             int compression_level) {
  if (!IsAvailable(codec_type)) {
    if (codec_type == Compression::LZO) {
      return Status::NotImplemented("LZO codec not implemented");
    }

    auto name = GetCodecAsString(codec_type);
    if (name == "unknown") {
      return Status::Invalid("Unrecognized codec");
    }

    return Status::NotImplemented("Support for codec '", GetCodecAsString(codec_type),
                                  "' not built");
  }

  if (compression_level != kUseDefaultCompressionLevel &&
      !SupportsCompressionLevel(codec_type)) {
    return Status::Invalid("Codec '", GetCodecAsString(codec_type),
                           "' doesn't support setting a compression level.");
  }

  std::unique_ptr<Codec> codec;
  switch (codec_type) {
    case Compression::UNCOMPRESSED:
      return nullptr;
    case Compression::SNAPPY:
#ifdef ARROW_WITH_SNAPPY
      codec = internal::MakeSnappyCodec();
#endif
      break;
    case Compression::GZIP:
#ifdef ARROW_WITH_ZLIB
      codec = internal::MakeGZipCodec(compression_level);
#endif
      break;
    case Compression::BROTLI:
#ifdef ARROW_WITH_BROTLI
      codec = internal::MakeBrotliCodec(compression_level);
#endif
      break;
    case Compression::LZ4:
#ifdef ARROW_WITH_LZ4
      codec = internal::MakeLz4RawCodec();
#endif
      break;
    case Compression::LZ4_FRAME:
#ifdef ARROW_WITH_LZ4
      codec = internal::MakeLz4FrameCodec();
#endif
      break;
    case Compression::LZ4_HADOOP:
#ifdef ARROW_WITH_LZ4
      codec = internal::MakeLz4HadoopRawCodec();
#endif
      break;
    case Compression::ZSTD:
#ifdef ARROW_WITH_ZSTD
      codec = internal::MakeZSTDCodec(compression_level);
#endif
      break;
    case Compression::BZ2:
#ifdef ARROW_WITH_BZ2
      codec = internal::MakeBZ2Codec(compression_level);
#endif
      break;
    default:
      break;
  }

  DCHECK_NE(codec, nullptr);
  RETURN_NOT_OK(codec->Init());
  return std::move(codec);
}

Result<std::unique_ptr<Codec>> Codec::CreateInt32(Compression::type codec_type,
                                                  int compression_level) {
  return CreateByType<uint32_t>(codec_type, compression_level);
}

Result<std::unique_ptr<Codec>> Codec::CreateInt64(Compression::type codec_type,
                                                  int compression_level) {
  return CreateByType<uint64_t>(codec_type, compression_level);
}

template <typename T>
Result<std::unique_ptr<Codec>> Codec::CreateByType(Compression::type codec_type,
                                                   int compression_level) {
  std::unique_ptr<Codec> codec;
  const bool compression_level_set{compression_level != kUseDefaultCompressionLevel};
  switch (codec_type) {
    case Compression::UNCOMPRESSED:
      if (compression_level_set) {
        return Status::Invalid("Compression level cannot be specified for UNCOMPRESSED.");
      }
      return nullptr;
    case Compression::FASTPFOR:
#ifdef ARROW_WITH_FASTPFOR
      if (compression_level_set) {
        return Status::Invalid("LZ4 doesn't support setting a compression level.");
      }
      codec = internal::MakeFastPForCodec<T>();
      break;
#else
      return Status::NotImplemented("FastPFor codec support not built");
#endif
    default:
      return Status::Invalid("Unrecognized codec");
  }
  RETURN_NOT_OK(codec->Init());
  return std::move(codec);
}

bool Codec::IsAvailable(Compression::type codec_type) {
  switch (codec_type) {
    case Compression::UNCOMPRESSED:
      return true;
    case Compression::SNAPPY:
#ifdef ARROW_WITH_SNAPPY
      return true;
#else
      return false;
#endif
    case Compression::GZIP:
#ifdef ARROW_WITH_ZLIB
      return true;
#else
      return false;
#endif
    case Compression::LZO:
      return false;
    case Compression::BROTLI:
#ifdef ARROW_WITH_BROTLI
      return true;
#else
      return false;
#endif
    case Compression::LZ4:
    case Compression::LZ4_FRAME:
    case Compression::LZ4_HADOOP:
#ifdef ARROW_WITH_LZ4
      return true;
#else
      return false;
#endif
    case Compression::ZSTD:
#ifdef ARROW_WITH_ZSTD
      return true;
#else
      return false;
#endif
    case Compression::BZ2:
#ifdef ARROW_WITH_BZ2
      return true;
#else
      return false;
#endif
    case Compression::FASTPFOR:
#ifdef ARROW_WITH_FASTPFOR
      return true;
#else
      return false;
#endif
    default:
      return false;
  }
}

}  // namespace util
}  // namespace arrow
