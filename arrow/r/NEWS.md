<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# arrow 3.0.0

## Python and Flight

* Flight methods `flight_get()` and `flight_put()` (renamed from `push_data()` in this release) can handle both Tables and RecordBatches
* `flight_put()` gains an `overwrite` argument to optionally check for the existence of a resource with the the same name
* `list_flights()` and `flight_path_exists()` enable you to see available resources on a Flight server
* `Schema` objects now have `r_to_py` and `py_to_r` methods
* Schema metadata is correctly preserved when converting Tables to/from Python

## Enhancements

* Arithmetic operations (`+`, `*`, etc.) are supported on Arrays and ChunkedArrays and can be used in filter expressions in Arrow `dplyr` pipelines
* Table columns can now be added, replaced, or removed by assigning (`<-`) with either `$` or `[[`
* Column names of Tables and RecordBatches can be renamed by assigning `names()`
* Large string types can now be written to Parquet files
* The [pronouns `.data` and `.env`](https://rlang.r-lib.org/reference/tidyeval-data.html) are now fully supported in Arrow `dplyr` pipelines.
* Option `arrow.skip_nul` (default `FALSE`, as in `base::scan()`) allows conversion of Arrow string (`utf8()`) type data containing embedded nul `\0` characters to R. If set to `TRUE`, nuls will be stripped and a warning is emitted if any are found.
* `arrow_info()` for an overview of various run-time and build-time Arrow configurations, useful for debugging
* Set environment variable `ARROW_DEFAULT_MEMORY_POOL` before loading the Arrow package to change memory allocators. Windows packages are built with `mimalloc`; most others are built with both `jemalloc` (used by default) and `mimalloc`. These alternative memory allocators are generally much faster than the system memory allocator, so they are used by default when available, but sometimes it is useful to turn them off for debugging purposes. To disable them, set `ARROW_DEFAULT_MEMORY_POOL=system`.
* List columns that have attributes on each element are now also included with the metadata that is saved when creating Arrow tables. This allows `sf` tibbles to faithfully preserved and roundtripped (ARROW-10386)[https://issues.apache.org/jira/browse/ARROW-10386].
* R metadata that exceeds 100Kb is now compressed before being written to a table; see `schema()` for more details.

## Bug fixes

* Fixed a performance regression in converting Arrow string types to R that was present in the 2.0.0 release
* C++ functions now trigger garbage collection when needed
* `write_parquet()` can now write RecordBatches
* Reading a Table from a RecordBatchStreamReader containing 0 batches no longer crashes
* `readr`'s `problems` attribute is removed when converting to Arrow RecordBatch and table to prevent large amounts of metadata from accumulating inadvertently [ARROW-10624](https://issues.apache.org/jira/browse/ARROW-10624)
* Fixed reading of compressed Feather files written with Arrow 0.17 [ARROW-10850](https://issues.apache.org/jira/browse/ARROW-10850)
* `SubTreeFileSystem` gains a useful print method and no longer errors when printing

## Packaging and installation

* Nightly development versions of the conda `r-arrow` package are available with `conda install -c arrow-nightlies -c conda-forge --strict-channel-priority r-arrow`
* Linux installation now safely supports older `cmake` versions
* Compiler version checking for enabling S3 support correctly identifies the active compiler
* Updated guidance and troubleshooting in `vignette("install", package = "arrow")`, especially for known CentOS issues
* Operating system detection on Linux uses the [`distro`](https://enpiar.com/distro/) package. If your OS isn't correctly identified, please report an issue there.

# arrow 2.0.0

## Datasets

* `write_dataset()` to Feather or Parquet files with partitioning. See the end of `vignette("dataset", package = "arrow")` for discussion and examples.
* Datasets now have `head()`, `tail()`, and take (`[`) methods. `head()` is optimized but the others  may not be performant.
* `collect()` gains an `as_data_frame` argument, default `TRUE` but when `FALSE` allows you to evaluate the accumulated `select` and `filter` query but keep the result in Arrow, not an R `data.frame`
* `read_csv_arrow()` supports specifying column types, both with a `Schema` and with the compact string representation for types used in the `readr` package. It also has gained a `timestamp_parsers` argument that lets you express a set of `strptime` parse strings that will be tried to convert columns designated as `Timestamp` type.

## AWS S3 support

* S3 support is now enabled in binary macOS and Windows (Rtools40 only, i.e. R >= 4.0) packages. To enable it on Linux, you need the additional system dependencies `libcurl` and `openssl`, as well as a sufficiently modern compiler. See `vignette("install", package = "arrow")` for details.
* File readers and writers (`read_parquet()`, `write_feather()`, et al.), as well as `open_dataset()` and `write_dataset()`, allow you to access resources on S3 (or on file systems that emulate S3) either by providing an `s3://` URI or by providing a `FileSystem$path()`. See `vignette("fs", package = "arrow")` for examples.
* `copy_files()` allows you to recursively copy directories of files from one file system to another, such as from S3 to your local machine.

## Flight RPC

[Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/)
is a general-purpose client-server framework for high performance
transport of large datasets over network interfaces.
The `arrow` R package now provides methods for connecting to Flight RPC servers
to send and receive data. See `vignette("flight", package = "arrow")` for an overview.

## Computation

* Comparison (`==`, `>`, etc.) and boolean (`&`, `|`, `!`) operations, along with `is.na`, `%in%` and `match` (called `match_arrow()`), on Arrow Arrays and ChunkedArrays are now implemented in the C++ library.
* Aggregation methods `min()`, `max()`, and `unique()` are implemented for Arrays and ChunkedArrays.
* `dplyr` filter expressions on Arrow Tables and RecordBatches are now evaluated in the C++ library, rather than by pulling data into R and evaluating. This yields significant performance improvements.
* `dim()` (`nrow`) for dplyr queries on Table/RecordBatch is now supported

## Packaging and installation

* `arrow` now depends on [`cpp11`](https://cpp11.r-lib.org/), which brings more robust UTF-8 handling and faster compilation
* The Linux build script now succeeds on older versions of R
* MacOS binary packages now ship with zstandard compression enabled

## Bug fixes and other enhancements

* Automatic conversion of Arrow `Int64` type when all values fit with an R 32-bit integer now correctly inspects all chunks in a ChunkedArray, and this conversion can be disabled (so that `Int64` always yields a `bit64::integer64` vector) by setting `options(arrow.int64_downcast = FALSE)`.
* In addition to the data.frame column metadata preserved in round trip, added in 1.0.0, now attributes of the data.frame itself are also preserved in Arrow schema metadata.
* File writers now respect the system umask setting
* `ParquetFileReader` has additional methods for accessing individual columns or row groups from the file
* Various segfaults fixed: invalid input in `ParquetFileWriter`; invalid `ArrowObject` pointer from a saved R object; converting deeply nested structs from Arrow to R
* The `properties` and `arrow_properties` arguments to `write_parquet()` are deprecated

# arrow 1.0.1

## Bug fixes

* Filtering a Dataset that has multiple partition keys using an `%in%` expression now faithfully returns all relevant rows
* Datasets can now have path segments in the root directory that start with `.` or `_`; files and subdirectories starting with those prefixes are still ignored
* `open_dataset("~/path")` now correctly expands the path
* The `version` option to `write_parquet()` is now correctly implemented
* An UBSAN failure in the `parquet-cpp` library has been fixed
* For bundled Linux builds, the logic for finding `cmake` is more robust, and you can now specify a `/path/to/cmake` by setting the `CMAKE` environment variable

# arrow 1.0.0

## Arrow format conversion

* `vignette("arrow", package = "arrow")` includes tables that explain how R types are converted to Arrow types and vice versa.
* Support added for converting to/from more Arrow types: `uint64`, `binary`, `fixed_size_binary`, `large_binary`, `large_utf8`, `large_list`, `list` of `structs`.
* `character` vectors that exceed 2GB are converted to Arrow `large_utf8` type
* `POSIXlt` objects can now be converted to Arrow (`struct`)
* R `attributes()` are preserved in Arrow metadata when converting to Arrow RecordBatch and table and are restored when converting from Arrow. This means that custom subclasses, such as `haven::labelled`, are preserved in round trip through Arrow.
* Schema metadata is now exposed as a named list, and it can be modified by assignment like `batch$metadata$new_key <- "new value"`
* Arrow types `int64`, `uint32`, and `uint64` now are converted to R `integer` if all values fit in bounds
* Arrow `date32` is now converted to R `Date` with `double` underlying storage. Even though the data values themselves are integers, this provides more strict round-trip fidelity
* When converting to R `factor`, `dictionary` ChunkedArrays that do not have identical dictionaries are properly unified
* In the 1.0 release, the Arrow IPC metadata version is increased from V4 to V5. By default, `RecordBatch{File,Stream}Writer` will write V5, but you can specify an alternate `metadata_version`. For convenience, if you know the consumer you're writing to cannot read V5, you can set the environment variable `ARROW_PRE_1_0_METADATA_VERSION=1` to write V4 without changing any other code.

## Datasets

* CSV and other text-delimited datasets are now supported
* With a custom C++ build, it is possible to read datasets directly on S3 by passing a URL like `ds <- open_dataset("s3://...")`. Note that this currently requires a special C++ library build with additional dependencies--this is not yet available in CRAN releases or in nightly packages.
* When reading individual CSV and JSON files, compression is automatically detected from the file extension

## Other enhancements

* Initial support for C++ aggregation methods: `sum()` and `mean()` are implemented for `Array` and `ChunkedArray`
* Tables and RecordBatches have additional data.frame-like methods, including `dimnames()` and `as.list()`
* Tables and ChunkedArrays can now be moved to/from Python via `reticulate`

## Bug fixes and deprecations

* Non-UTF-8 strings (common on Windows) are correctly coerced to UTF-8 when passing to Arrow memory and appropriately re-localized when converting to R
* The `coerce_timestamps` option to `write_parquet()` is now correctly implemented.
* Creating a Dictionary array respects the `type` definition if provided by the user
* `read_arrow` and `write_arrow` are now deprecated; use the `read/write_feather()` and `read/write_ipc_stream()` functions depending on whether you're working with the Arrow IPC file or stream format, respectively.
* Previously deprecated `FileStats`, `read_record_batch`, and `read_table` have been removed.

## Installation and packaging

* For improved performance in memory allocation, macOS and Linux binaries now have `jemalloc` included, and Windows packages use `mimalloc`
* Linux installation: some tweaks to OS detection for binaries, some updates to known installation issues in the vignette
* The bundled libarrow is built with the same `CC` and `CXX` values that R uses
* Failure to build the bundled libarrow yields a clear message
* Various streamlining efforts to reduce library size and compile time

# arrow 0.17.1

* Updates for compatibility with `dplyr` 1.0
* `reticulate::r_to_py()` conversion now correctly works automatically, without having to call the method yourself
* Assorted bug fixes in the C++ library around Parquet reading

# arrow 0.17.0

## Feather v2

This release includes support for version 2 of the Feather file format.
Feather v2 features full support for all Arrow data types,
fixes the 2GB per-column limitation for large amounts of string data,
and it allows files to be compressed using either `lz4` or `zstd`.
`write_feather()` can write either version 2 or
[version 1](https://github.com/wesm/feather) Feather files, and `read_feather()`
automatically detects which file version it is reading.

Related to this change, several functions around reading and writing data
have been reworked. `read_ipc_stream()` and `write_ipc_stream()` have been
added to facilitate writing data to the Arrow IPC stream format, which is
slightly different from the IPC file format (Feather v2 *is* the IPC file format).

Behavior has been standardized: all `read_<format>()` return an R `data.frame`
(default) or a `Table` if the argument `as_data_frame = FALSE`;
all `write_<format>()` functions return the data object, invisibly.
To facilitate some workflows, a special `write_to_raw()` function is added
to wrap `write_ipc_stream()` and return the `raw` vector containing the buffer
that was written.

To achieve this standardization, `read_table()`, `read_record_batch()`,
`read_arrow()`, and `write_arrow()` have been deprecated.

## Python interoperability

The 0.17 Apache Arrow release includes a C data interface that allows
exchanging Arrow data in-process at the C level without copying
and without libraries having a build or runtime dependency on each other. This enables
us to use `reticulate` to share data between R and Python (`pyarrow`) efficiently.

See `vignette("python", package = "arrow")` for details.

## Datasets

* Dataset reading benefits from many speedups and fixes in the C++ library
* Datasets have a `dim()` method, which sums rows across all files (#6635, @boshek)
* Combine multiple datasets into a single queryable `UnionDataset` with the `c()` method
* Dataset filtering now treats `NA` as `FALSE`, consistent with `dplyr::filter()`
* Dataset filtering is now correctly supported for all Arrow date/time/timestamp column types
* `vignette("dataset", package = "arrow")` now has correct, executable code

## Installation

* Installation on Linux now builds C++ the library from source by default, with some compression libraries disabled. For a faster, richer build, set the environment variable `NOT_CRAN=true`. See `vignette("install", package = "arrow")` for details and more options.
* Source installation is faster and more reliable on more Linux distributions.

## Other bug fixes and enhancements

* `unify_schemas()` to create a `Schema` containing the union of fields in multiple schemas
* Timezones are faithfully preserved in roundtrip between R and Arrow
* `read_feather()` and other reader functions close any file connections they open
* Arrow R6 objects no longer have namespace collisions when the `R.oo` package is also loaded
* `FileStats` is renamed to `FileInfo`, and the original spelling has been deprecated

# arrow 0.16.0.2

* `install_arrow()` now installs the latest release of `arrow`, including Linux dependencies, either for CRAN releases or for development builds (if `nightly = TRUE`)
* Package installation on Linux no longer downloads C++ dependencies unless the `LIBARROW_DOWNLOAD` or `NOT_CRAN` environment variable is set
* `write_feather()`, `write_arrow()` and `write_parquet()` now return their input,
similar to the `write_*` functions in the `readr` package (#6387, @boshek)
* Can now infer the type of an R `list` and create a ListArray when all list elements are the same type (#6275, @michaelchirico)

# arrow 0.16.0

## Multi-file datasets

This release includes a `dplyr` interface to Arrow Datasets,
which let you work efficiently with large, multi-file datasets as a single entity.
Explore a directory of data files with `open_dataset()` and then use `dplyr` methods to `select()`, `filter()`, etc. Work will be done where possible in Arrow memory. When necessary, data is pulled into R for further computation. `dplyr` methods are conditionally loaded if you have `dplyr` available; it is not a hard dependency.

See `vignette("dataset", package = "arrow")` for details.

## Linux installation

A source package installation (as from CRAN) will now handle its C++ dependencies automatically.
For common Linux distributions and versions, installation will retrieve a prebuilt static
C++ library for inclusion in the package; where this binary is not available,
the package executes a bundled script that should build the Arrow C++ library with
no system dependencies beyond what R requires.

See `vignette("install", package = "arrow")` for details.

## Data exploration

* `Table`s and `RecordBatch`es also have `dplyr` methods.
* For exploration without `dplyr`, `[` methods for Tables, RecordBatches, Arrays, and ChunkedArrays now support natural row extraction operations. These use the C++ `Filter`, `Slice`, and `Take` methods for efficient access, depending on the type of selection vector.
* An experimental, lazily evaluated `array_expression` class has also been added, enabling among other things the ability to filter a Table with some function of Arrays, such as `arrow_table[arrow_table$var1 > 5, ]` without having to pull everything into R first.

## Compression

* `write_parquet()` now supports compression
* `codec_is_available()` returns `TRUE` or `FALSE` whether the Arrow C++ library was built with support for a given compression library (e.g. gzip, lz4, snappy)
* Windows builds now include support for zstd and lz4 compression (#5814, @gnguy)

## Other fixes and improvements

* Arrow null type is now supported
* Factor types are now preserved in round trip through Parquet format (#6135, @yutannihilation)
* Reading an Arrow dictionary type coerces dictionary values to `character` (as R `factor` levels are required to be) instead of raising an error
* Many improvements to Parquet function documentation (@karldw, @khughitt)

# arrow 0.15.1

* This patch release includes bugfixes in the C++ library around dictionary types and Parquet reading.

# arrow 0.15.0

## Breaking changes

* The R6 classes that wrap the C++ classes are now documented and exported and have been renamed to be more R-friendly. Users of the high-level R interface in this package are not affected. Those who want to interact with the Arrow C++ API more directly should work with these objects and methods. As part of this change, many functions that instantiated these R6 objects have been removed in favor of `Class$create()` methods. Notably, `arrow::array()` and `arrow::table()` have been removed in favor of `Array$create()` and `Table$create()`, eliminating the package startup message about masking `base` functions. For more information, see the new `vignette("arrow")`.
* Due to a subtle change in the Arrow message format, data written by the 0.15 version libraries may not be readable by older versions. If you need to send data to a process that uses an older version of Arrow (for example, an Apache Spark server that hasn't yet updated to Arrow 0.15), you can set the environment variable `ARROW_PRE_0_15_IPC_FORMAT=1`.
* The `as_tibble` argument in the `read_*()` functions has been renamed to `as_data_frame` ([ARROW-6337](https://issues.apache.org/jira/browse/ARROW-6337), @jameslamb)
* The `arrow::Column` class has been removed, as it was removed from the C++ library

## New features

* `Table` and `RecordBatch` objects have S3 methods that enable you to work with them more like `data.frame`s. Extract columns, subset, and so on. See `?Table` and `?RecordBatch` for examples.
* Initial implementation of bindings for the C++ File System API. ([ARROW-6348](https://issues.apache.org/jira/browse/ARROW-6348))
* Compressed streams are now supported on Windows ([ARROW-6360](https://issues.apache.org/jira/browse/ARROW-6360)), and you can also specify a compression level ([ARROW-6533](https://issues.apache.org/jira/browse/ARROW-6533))

## Other upgrades

* Parquet file reading is much, much faster, thanks to improvements in the Arrow C++ library.
* `read_csv_arrow()` supports more parsing options, including `col_names`, `na`, `quoted_na`, and `skip`
* `read_parquet()` and `read_feather()` can ingest data from a `raw` vector ([ARROW-6278](https://issues.apache.org/jira/browse/ARROW-6278))
* File readers now properly handle paths that need expanding, such as `~/file.parquet` ([ARROW-6323](https://issues.apache.org/jira/browse/ARROW-6323))
* Improved support for creating types in a schema: the types' printed names (e.g. "double") are guaranteed to be valid to use in instantiating a schema (e.g. `double()`), and time types can be created with human-friendly resolution strings ("ms", "s", etc.). ([ARROW-6338](https://issues.apache.org/jira/browse/ARROW-6338), [ARROW-6364](https://issues.apache.org/jira/browse/ARROW-6364))


# arrow 0.14.1

Initial CRAN release of the `arrow` package. Key features include:

* Read and write support for various file formats, including Parquet, Feather/Arrow, CSV, and JSON.
* API bindings to the C++ library for Arrow data types and objects, as well as mapping between Arrow types and R data types.
* Tools for helping with C++ library configuration and installation.
