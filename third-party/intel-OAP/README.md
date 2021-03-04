# Intel OAP
The Intel OAP project is open-sourced, licensed under the permissive [Apache 2.0 license](/third-party/licenses/apache2.0.txt).

In this work, we adapted a small part of the Intel Optimized Analytics Platform project, found [here](https://github.com/Intel-bigdata/OAP).
In particular, we changed the following files. For each file, we describe in detail what we changed, as under the Apache 2.0 license agreement:
 1. [`oap-data-source/arrow/standard/src/main/scala/com/intel/oap/spark/sql/execution/datasources/arrow/ArrowFileFormat.scala`](https://github.com/Intel-bigdata/OAP/blob/master/oap-data-source/arrow/standard/src/main/scala/com/intel/oap/spark/sql/execution/datasources/arrow/ArrowFileFormat.scala).  
    Our related file is [here](/arrow-spark-connector/src/main/scala/org/apache/spark/sql/execution/datasources/arrow/ArrowFileFormat.scala).
    Implemented code to switch between schema merge strategies. 
    Added implicit functions to allow DPS practitioners to construct this format like they are familiar with.
    Changed all code related to the main functionality of this object (reading data).
    Removed filter translations. 
 2. [`oap-data-source/arrow/standard/src/main/scala/com/intel/oap/spark/sql/execution/datasources/v2/arrow/ArrowOptions.scala`](https://github.com/Intel-bigdata/OAP/blob/master/oap-data-source/arrow/standard/src/main/scala/com/intel/oap/spark/sql/execution/datasources/v2/arrow/ArrowOptions.scala).  
    Our related file is [here](/arrow-spark-connector/src/main/scala/org/apache/spark/sql/util/ArrowOptions.scala).
    Completely changed all options provided. Added support for default options.
 3. [`oap-data-source/arrow/common/src/main/scala/org/apache/spark/sql/execution/datasources/v2/arrow/SparkMemoryUtils.scala`](https://github.com/Intel-bigdata/OAP/blob/master/oap-data-source/arrow/common/src/main/scala/org/apache/spark/sql/execution/datasources/v2/arrow/SparkMemoryUtils.scala).  
    Our related file is [here](/arrow-spark-connector/src/main/scala/org/apache/arrow/memory/MemoryUtil.scala).
    Added support for on-heap/off-heap memory allocations.
 4. [`oap-data-source/arrow/standard/src/main/scala/com/intel/oap/spark/sql/execution/datasources/v2/arrow/ArrowUtils.scala`](https://github.com/Intel-bigdata/OAP/blob/master/oap-data-source/arrow/standard/src/main/scala/com/intel/oap/spark/sql/execution/datasources/v2/arrow/ArrowUtils.scala).  
    Our related files are 
    [1](/arrow-spark-connector/src/main/scala/org/apache/spark/sql/util/ArrowDatasetUtil.scala),
    [2](/arrow-spark-connector/src/main/scala/org/apache/spark/sql/util/conversion/SchemaConversionUtil.scala),
    [3](/arrow-spark-connector/src/main/scala/org/apache/spark/sql/util/conversion/TypeConversionUtils.scala).
    Changed code to not work with FileSystem-differentiation.
    Renamed all utility functions.
    Added read support for non-splitable fileformats, such as CSV.
    Added read support for CSV.
    Split schema conversion to files 2, 3.

We thank the Intel Corporation for open-sourcing the source code for this project, allowing us to build upon their work.