# Arrow-Spark
A connector system to connect Distributed Data Processing system [Apache Spark](https://org.apache.spark.apache.org/) with in-memory analytics platform [Arrow](https://arrow.apache.org/).

Specifically, we provide this connector between the new Arrow `Dataset` API and Spark, using an experimental Java Native Interface [bridge](https://github.com/Sebastiaan-Alvarez-Rodriguez/arrow/tree/OAP-1852-DAS5).
The idea behind this is that we no longer have to rely on Spark's implementations to fetch data.

This feature is nice, because the Arrow Dataset API
 1. will support more data types
 2. reads faster on some data types (e.g. CSV) and just as fast on others (e.g. parquet)
 3. allows us to interact with any framework that is able to produce some Arrow IPC format

The following features are advertised by Arrow Dataset API:
 1. A unified interface for different sources: supporting different sources and file formats (Parquet, Feather files) and different file systems (local, cloud).
 2. Discovery of sources (crawling directories, handle directory-based partitioned datasets, basic schema normalization, ...)
 3. Optimized reading with predicate pushdown (filtering rows), projection (selecting columns), parallel reading or fine-grained managing of tasks.
> [source](https://arrow.apache.org/docs/python/dataset.html#reading-from-cloud-storage)

other than these benefits, using Arrow instead of Spark makes loading a lot faster.

There is little documentation on the Dataset API for C++ or JVM languages.
As the development of mainly the Java Native Interface [bridge](https://github.com/zhztheplayer/arrow-1/tree/ARROW-7808) becomes more and more stable, we will add more features to our connector.
While the Arrow Dataset API remains in alpha, regard this project more as a working prototype, similarly in alpha as the functionality it brings to Spark.

## Building
Installation instructions are in [INSTALL.md](/INSTALL.md).


## Basic Usage
The normal Spark way is this:
```scala
val csv_df: DataFrame = session.read.format("csv").load("file:/some/path.csv")
val pq_df: DataFrame = session.read.parquet("hdfs:/another/path.pq")
```

With our connector, we can use:
```scala
val config: Config = Config.builder().withSparkConf(sc.getConf)
            .withFileFormat(FileFormat.PARQUET)
            .withDataSource(source)
            .build()
val rdd: ArrowRDD = ArrowSpark.load(session.sparkContext, conf)
```
for RDDs. For Dataframes, Spark Datasets, we use:
```scala
val df: DataFrame = session.read.arrow(path)
```

In both cases, is not required to set the fileformat. This will be automatically determined.  
In case the system cannot determine the format, or picked the wrong one, you can set it yourself using:
```scala
val config: Config = Config.builder().withSparkConf(sc.getConf)
            .withFileFormat(FileFormat.PARQUET) // Sets FileFormat manually
            .withDataSource(source)
            .build()
```
for RDDs. For Dataframes, Spark Datasets, we specify the fileformat using:
```
session.read.option("arrow.read.fileFormat", "parquet").arrow(path)
```
## Acknowledgements
Found in [CREDITS.md](/CREDITS.md).