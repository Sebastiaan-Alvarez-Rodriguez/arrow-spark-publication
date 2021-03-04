package org.arrowspark.benchmark

import org.apache.arrow.dataset.file.FileFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.arrow.ArrowDataframeReader
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.arrowspark.arrow.dataset.DatasetFileFormat
import org.arrowspark.benchmark.Benchmark.{Excl1, getSession}
import org.arrowspark.file.{CsvSupport, FileSupport, ParquetSupport}
import org.arrowspark.spark.ArrowSpark
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.ArrowRDD
import org.arrowspark.spark.rdd.partitioner.file.FilePartitioner
import picocli.CommandLine
import picocli.CommandLine.{Command, Parameters}

import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors, ThreadLocalRandom, TimeUnit}
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.util.Try

/** Object to handle experimentation */
object Benchmark extends Logging {
    /**
     * @param name Experiment name
     * @param testing User-provided option: If set, this number of processors will be used to do a local test
     * @return Generated sparksession
     */
    private def getSession(name: String, testing: Option[String]) = testing match {
        case Some(value) => SparkSession.builder.appName(name).config("spark.master", s"local[$value]").getOrCreate()//.config("spark.memory.offHeap.size", String.valueOf(1000L*1024*1024)).config("spark.memory.offHeap.enabled", "true")
        case None => SparkSession.builder.appName(name).getOrCreate()
    }

    /** @return A DF containing the data, default Spark */
    private def getSparkDF(fileFormat: DatasetFileFormat, session: SparkSession, path: String, numCols: Int): DataFrame = fileFormat.format match {
        case FileFormat.PARQUET => session.read.format("parquet").load(path)
        case FileFormat.CSV => session.read.format("csv").option("header", "true").option("inferschema", "true").schema(sparkSchema(numCols)).load(path)
        case _ => throw new RuntimeException(s"Cannot use unknown format '$fileFormat'")
    }

    /** @return A DF containing the data, through Arrow */
    private def getArrowDF(fileFormat: DatasetFileFormat, session: SparkSession, path: String): DataFrame = fileFormat.format match {
        case FileFormat.PARQUET | FileFormat.CSV => session.read.arrow(path)
        case _ => throw new RuntimeException(s"Cannot use unknown format '$fileFormat'")
    }

    /** @return A RDD containing the data, using our low-level connector  */
    private def getArrowRDD(fileFormat: DatasetFileFormat, session: SparkSession, path: String, batchSize: Long, partitions: Int, dataMultiplier: Int): ArrowRDD = {
        val conf = getArrowConf(session.sparkContext.getConf, path, fileFormat, batchSize, partitions, dataMultiplier)
        ArrowSpark.load(session.sparkContext, conf)
    }

    /** @return Arrow config for RDD, set with relevant experimentation parameters */
    private def getArrowConf(conf: SparkConf, path: String, fileFormat: DatasetFileFormat, batchSize: Long, partitions: Int, dataMultiplier: Int): ArrowRDDReadConfig = ArrowRDDReadConfig.builder()
        .withSparkConf(conf)
        .withPartitioner(FilePartitioner)
        .withFileFormat(fileFormat)
        .withOption(s"${ArrowRDDReadConfig.datasetProviderOptionsProperty}.${ArrowRDDReadConfig.datasetProviderOptionsPathExtraProperty}", s"0.${fileFormat.toExtensionString}")
        .withOption(s"${ArrowRDDReadConfig.datasetProviderOptionsProperty}.${ArrowRDDReadConfig.partitionerOptionsDataMultiplierProperty}", dataMultiplier.toString)
        .withNumPartitions(partitions)
        .withBatchSize(batchSize)
        .withDataSourceURI("file://"+path)
        .build()



    /** Forcefully triggers execution for RDDs */
    //noinspection ScalaUnusedSymbol
    def triggerExecution(rdd: RDD[_]): Unit = {
        val x = rdd.count()
    }
    /** Forcefully triggers execution for Datasets (and for Dataframes too, as a `DataFrame` simply is another term for `Dataset[Row]`) */
    //noinspection ScalaUnusedSymbol
    def triggerExecution(ds: Dataset[_]): Unit = {
        val plan = ds.queryExecution.executedPlan.execute()
        val x: Long = plan.count()
//        plan.foreachPartition(p => println("partition size = " + p.length))
    }

    /** Prints query plan explanation for Datasets (and for Dataframes too, as a `DataFrame` is simply another term for `Dataset[Row]` */
    def explain(ds: Dataset[_], extended: Boolean): Unit = ds.explain(extended)

    /** Spark: RDD operations using FileFormat, very slow */
    private def rddSpark(session: SparkSession, path: String, fileFormat: DatasetFileFormat, numCols: Int, computeCols: Int, compute: Boolean, partitions: Int): Unit = {
        val initstart: Long = System.nanoTime()
        val rdd: RDD[Row] = getSparkDF(fileFormat, session, path, numCols).rdd
        val inittime: Long = System.nanoTime()-initstart

        val computestart: Long = System.nanoTime()
        val untriggeredRDD =
            if (compute)
                rdd.map(row => (0 until computeCols).map(x => row.getLong(x)).sum)
            else
                rdd.map(row => Tuple4(row.getLong(0), row.getLong(1), row.getLong(2), row.getLong(3)))
        triggerExecution(untriggeredRDD)
        val computetime: Long = System.nanoTime() - computestart
        
        logInfo(s"$inittime,$computetime\n")
    }



    /** Arrow: RDD operations using FileFormat, very slow */
    //noinspection ScalaUnusedSymbol
    private def rddArrowSlow(session: SparkSession, path: String, fileFormat: DatasetFileFormat, computeCols: Int, compute: Boolean, partitions: Int): Unit = {
        val initstart: Long = System.nanoTime()
        var rdd: RDD[Row] = getArrowDF(fileFormat, session, path).rdd
        val inittime: Long = System.nanoTime()-initstart


        val computestart: Long = System.nanoTime()
        val untriggeredRDD =
            if (compute)
                rdd.map(row => (0 until computeCols).map(x => row.getLong(x)).sum)
            else
                rdd.map(row => Tuple4(row.getLong(0), row.getLong(1), row.getLong(2), row.getLong(3)))
        triggerExecution(untriggeredRDD)
        val computetime: Long = System.nanoTime() - computestart
        
        logInfo(s"$inittime,$computetime\n")
    }


    /** Arrow: RDD operations benchmark */
    private def rddArrow(session: SparkSession, path: String, fileFormat: DatasetFileFormat, computeCols: Int, compute: Boolean, partitions: Int, dataMultiplier: Int): Unit = {
        val batchSize = session.sessionState.conf.parquetVectorizedReaderBatchSize

        val initstart: Long = System.nanoTime()
        val rdd: ArrowRDD = getArrowRDD(fileFormat: DatasetFileFormat, session, path, batchSize, partitions, dataMultiplier)
        val inittime: Long = System.nanoTime()-initstart

        val computestart: Long = System.nanoTime()
        val untriggeredRDD =
            if (compute)
                rdd.map(row => (0 until computeCols).map(x => row.getLong(x)).sum)
            else
                rdd.map(row => Tuple4(row.getLong(0), row.getLong(1), row.getLong(2), row.getLong(3)))
        triggerExecution(untriggeredRDD)
        val computetime: Long = System.nanoTime() - computestart

        
        logInfo(s"$inittime,$computetime\n")
    }

    //////////////////////////////////////


    /** Spark: DF operations benchmark */
    private def dfSpark(session: SparkSession, path: String, fileFormat: DatasetFileFormat, computeCols: Int, compute: Boolean, numCols: Int, partitions: Int): Unit = {
        val initstart: Long = System.nanoTime()
        val df: DataFrame = getSparkDF(fileFormat, session, path, numCols)
        val inittime: Long = System.nanoTime()-initstart

        val computestart: Long = System.nanoTime()
        val untriggeredDF =
            if (compute)
                df.orderBy(org.apache.spark.sql.functions.asc("val3"))
            else
                df.select((0 until computeCols).map(x => col(s"val$x")):_*)
        triggerExecution(untriggeredDF)
        val computetime: Long = System.nanoTime()-computestart

        
       logInfo(s"$inittime,$computetime\n")
    }


    /** Arrow: DF operations benchmark */
    private def dfArrow(session: SparkSession, path: String, fileFormat: DatasetFileFormat, computeCols: Int, compute: Boolean, partitions: Int): Unit = {
        val initstart: Long = System.nanoTime()
        val df: DataFrame = getArrowDF(fileFormat, session, path)
        val inittime: Long = System.nanoTime()-initstart

        val computestart: Long = System.nanoTime()
        val untriggeredDF =
            if (compute)
                df.orderBy(org.apache.spark.sql.functions.asc("val3"))
            else
                df.select((0 until computeCols).map(x => col(s"val$x")):_*)
        triggerExecution(untriggeredDF)
        val computetime: Long = System.nanoTime()-computestart
        logInfo(s"$inittime,$computetime\n")
    }


    //////////////////////////////////////

    /** Spark: DF raw SQL operations benchmark */
    private def dfSparkSQL(session: SparkSession, path: String, fileFormat: DatasetFileFormat, computeCols: Int, compute: Boolean, numCols: Int, partitions: Int): Unit = {
        val initstart: Long = System.nanoTime()
        val df: DataFrame = getSparkDF(fileFormat, session, path, numCols)
        val inittime: Long = System.nanoTime()-initstart

        df.createOrReplaceTempView("mytable")
        val computestart: Long = System.nanoTime()
        val untriggeredDF =
            if (compute)
                session.sql(s"SELECT val3 FROM mytable ORDER BY val3")
            else
                session.sql(s"SELECT ${(0 until computeCols).map(x => s"val$x").mkString(",")} FROM mytable")
        triggerExecution(untriggeredDF)
        val computetime: Long = System.nanoTime()-computestart
        
       logInfo(s"$inittime,$computetime\n")
    }

    /** Arrow: DF raw SQL operations benchmark */
    private def dfArrowSQL(session: SparkSession, path: String, fileFormat: DatasetFileFormat, computeCols: Int, compute: Boolean, partitions: Int): Unit = {
        val initstart: Long = System.nanoTime()
        val df: DataFrame = getArrowDF(fileFormat, session, path)
        val inittime: Long = System.nanoTime()-initstart

        df.createOrReplaceTempView("mytable")
        val computestart: Long = System.nanoTime()
        val untriggeredDF: DataFrame =
            if (compute)
                session.sql(s"SELECT val3 FROM mytable ORDER BY val3")
            else
                session.sql(s"SELECT ${(0 until computeCols).map(x => s"val$x").mkString(",")} FROM mytable")
        triggerExecution(untriggeredDF)
        val computetime: Long = System.nanoTime()-computestart
        
        logInfo(s"$inittime,$computetime\n")
    }

    //////////////////////////////////////

    private def dsSpark(session: SparkSession, path: String, fileFormat: DatasetFileFormat, computeCols: Int, compute: Boolean, numCols: Int, partitions: Int): Unit = {
        val initstart: Long = System.nanoTime()
        val ds: Dataset[Contributor] = getSparkDF(fileFormat, session, path, numCols).as[Contributor](Encoders.product[Contributor])
        val inittime: Long = System.nanoTime()-initstart

        val computestart: Long = System.nanoTime()
        val untriggeredDS: Dataset[Contributor] =
            if (compute)
                ds.orderBy(org.apache.spark.sql.functions.asc("val3"))
            else
                ds.select(col("val0"), col("val1"), col("val2"), col("val3")).as[Contributor](Encoders.product[Contributor])
        triggerExecution(untriggeredDS)
        val computetime: Long = System.nanoTime()-computestart

       logInfo(s"$inittime,$computetime\n")
    }

    private def dsArrow(session: SparkSession, path: String, fileFormat: DatasetFileFormat, computeCols: Int, compute: Boolean, partitions: Int): Unit = {
        val initstart: Long = System.nanoTime()
        val ds: Dataset[Contributor] = getArrowDF(fileFormat, session, path).as[Contributor](Encoders.product[Contributor])
        val inittime: Long = System.nanoTime()-initstart

        val computestart: Long = System.nanoTime()
        val untriggeredDS: Dataset[Contributor] =
            if (compute)
                ds.orderBy(org.apache.spark.sql.functions.asc("val3"))
            else
                ds.select(col("val0"), col("val1"), col("val2"), col("val3")).as[Contributor](Encoders.product[Contributor])
        triggerExecution(untriggeredDS)
        val computetime: Long = System.nanoTime()-computestart

        logInfo(s"$inittime,$computetime\n")
    }

    //////////////////////////////////////

    /** The Schema of the data we write, given as an AVRO schema */
    private def AVROSchema(width: Int): String = {
        val header = """
          | "namespace": "org.apache.arrow.connector",
          | "type": "record",
          | "name": "User",
          | "fields": """.stripMargin
        val body = (0 until width).map(x => s"""{"name": "val$x", "type": ["long", "null"]}""").mkString("[", ",", "]")
        s"{$header$body}"
    }

    /** Schema columns expressed as java primitive types. Required for CSV file creation */
    private def primitiveSchema(width: Int): List[Class[_]] = (0 until width).map(_ => Class.forName("java.lang.Long")).toList

    /** Spark-Schema for our data. Spark CSV reader needs this to be able to read CSV in such a way that not everything is deducted to be StringType */
    private def sparkSchema(width: Int): StructType = StructType((0 until width).map(x => StructField(s"val$x", LongType)))

    /** CSV header. Required for CSV file creation */
    private def primitiveHeader(width: Int): String = (0 until width).map(x => s"val$x").mkString(",")
//
//    /** Simple class representing the record data, used for binding */
//    case class Contributor20(val0: Long, val1: Long, val2: Long, val3: Long, val4: Long, val5: Long, val6: Long, val7: Long, val8: Long, val9: Long, val10: Long, val11: Long, val12: Long, val13: Long, val14: Long, val15: Long, val16: Long, val17: Long, val18: Long, val19: Long) {
//        def sum: Long = val0+val1+val3+val4+val5+val6+val7+val8+val9+val10+val11+val12+val13+val14+val15+val16+val17+val18+val19
//        def num: Short = 20
//    }
    case object Contributor {
        def num: Short = 4
    }
    case class Contributor(val0: Long, val1: Long, val2: Long, val3: Long) {
        def sum: Long = val0+val1+val2+val3
        def sum(computeCols: Int): Long = List(val0, val1, val2, val3).slice(0, computeCols).sum
    }


    /**
     * Function providing a population buffer for writing a testfile.
     * Note: Make sure the values are of type as ordered in the rows of the schema.
     * E.g: If the schema specifies we have rows consisting of int, String, long, then returned arraybuffer must have a repeating sequence of those types, in that order.
     *
     * Currently generates rows of 4 longs, where a row looks like `x>>3, x>>2, x>>1, x`.
     * In case we generate non-random data, The next row will be `(x-1)>>3, (x-1)>>2, (x-1)>>1, (x-1)`.
     * Otherwise, when we generate random data, the next row will look like `y>>3, y>>2, y>>1, y`, with y some random number.
     */
    def generateTempData(amount: Long, start: Long, width: Int, random: Boolean=false): java.util.stream.Stream[Object] = {
        val end = start+amount
        if (random)
            ThreadLocalRandom.current().longs(amount).flatMap(value => java.util.stream.LongStream.range(0, width).map(shift => width-shift-1).map(shift_reverse => value >> shift_reverse)).boxed().asInstanceOf[java.util.stream.Stream[Object]]
        else
            java.util.stream.LongStream.range(start, start+amount).map(x => start+end-x-1).flatMap(value => java.util.stream.LongStream.range(0, width).map(shift => width-shift-1).map(shift_reverse => value >> shift_reverse)).boxed().asInstanceOf[java.util.stream.Stream[Object]]
    }

    def generateTempData(amount: Long, width: Int, random: Boolean): java.util.stream.Stream[Object] = generateTempData(amount, 0, width, random)


    /** Constructs full path to store data for given partition number */
    private def makeDataGenerationPath(dataPath: String, numRows: Long, dataFormat: DatasetFileFormat, compressionCodecName: CompressionCodecName, partitions: Int, random_generated: Boolean): String = {
        val extension = dataFormat.toExtensionString
        if (dataFormat.format == FileFormat.CSV || (dataFormat.format == FileFormat.PARQUET && compressionCodecName == CompressionCodecName.UNCOMPRESSED))
            s"$dataPath/$numRows/$partitions/${if (random_generated) "rnd" else ""}$extension/"
        else
            s"$dataPath/$numRows/$partitions/${if (random_generated) "rnd" else ""}${extension}_${ParquetSupport.compressionCodecToString(compressionCodecName)}/"
    }


    def main(args: Array[String]): Unit = {
        new CommandLine(new Benchmark())
            .registerConverter(classOf[Option[String]], s => Option(s))
            .registerConverter(classOf[DatasetFileFormat], (s: String) => DatasetFileFormat.fromString(s))
            .registerConverter(classOf[CompressionCodecName], (s: String) => ParquetSupport.compressionCodecFromString(s))
            .execute(args:_*)
    }

    class Excl1 {
        @picocli.CommandLine.Option(names = Array("--arrow-only"), description=Array("Only execute Arrow-reading code"), required = true)
        var onlyArrow: Boolean = false
        @picocli.CommandLine.Option(names = Array("--spark-only"), description=Array("Only execute Spark-default-reading code"), required = true)
        var onlySpark: Boolean = false
    }
    val programVersion = 3.0
}

//noinspection ScalaUnusedSymbol,VarCouldBeVal
@Command(name="Benchmark", mixinStandardHelpOptions=true, version=Array("3.0"), description=Array("Performs given benchmarks with given options"))
class Benchmark extends Callable[Unit] {
    @Parameters(description=Array("The experiment to perform: One of {rdd, df, df_sql, ds}"))
    private var targets: java.util.List[String] = new util.ArrayList[String]()


    @picocli.CommandLine.Option(names=Array("-p", "--path"), required = true, description=Array("Path to read/write data source files. Note: we will create subdirectories"))
    private var dataPath: String = "/tmp/RDDTest"

    @picocli.CommandLine.Option(names=Array("-f", "--format"), required = true, description=Array("Data format for data source. Allowed: {pq (parquet), csv (csv)}"))
    private var dataFormat: DatasetFileFormat = DatasetFileFormat()

    @picocli.CommandLine.Option(names=Array("-cl", "--compression-level"), description=Array("Compression schema to use when generating data. Only used for generating parquet data"))
    private var dataCompression: CompressionCodecName = CompressionCodecName.UNCOMPRESSED

    @picocli.CommandLine.Option(names=Array("-dm", "--data-multiplier"), description=Array("Data multiplier used when generating. Use integers >= 1}"))
    private var dataMultiplier: Int = 1

    @picocli.CommandLine.Option(names=Array("-gf", "--generate-force"), description=Array("Forces to generate and store test data at specified path, even when a data source file already exists"))
    private var generate_force: Boolean = false

    @picocli.CommandLine.Option(names=Array("-gr", "--generate-random"), description=Array("Generates semi-randomly shuffled arrays instead of normal ones"))
    private var generate_random: Boolean = false


    @picocli.CommandLine.Option(names=Array("-c", "--compute"), description=Array("If set, perform row-wise computation on dataset, otherwise just scan"))
    private var compute: Boolean = false

    @picocli.CommandLine.Option(names=Array("-nr", "--num_rows"), description=Array("Sets amount of rows which we write (if needed) into testfiles. This field is required when we test with a multi-file setup, because every"))
    private var numRows: Long = 0

    @picocli.CommandLine.Option(names=Array("-nc", "--num-cols"), description=Array("Number of columns to write (if needed) into the testfile"))
    private var numCols: Int = 4

    @picocli.CommandLine.Option(names=Array("-cc", "--compute-cols"), description=Array("Number of columns to use during computation. Other columns are simply not used. Useful to show column pruning capabilities!"))
    private var computeCols: Int = 4

    @picocli.CommandLine.Option(names=Array("-np", "--partitions"), description=Array("Set amount of partitions to use when computing"))
    private var partitions: Int = 4

    @picocli.CommandLine.Option(names=Array("-r", "--repeats"), description=Array("Amount of times to repeat the experiment. Setting this to a higher number will make measurements more statistically significant, but also increase runtime"))
    private var repeats: Int = 10


    @picocli.CommandLine.ArgGroup(exclusive=true)
    private var exclusiveExec: Excl1 = new Excl1

    @picocli.CommandLine.Option(names=Array("--test"), description=Array("Runs experiment on local machine instead of on cluster"))
    private var testing: Option[String] = None

    /**
     * Generates data we test on.
     *
     * @param singleFile If 'true', we generate 1 file. Otherwise, we generate 1 file for every partition and build a filestructure "datapath/num_partitions/extension/[0-num_partitions-1].extension"
     */
    private def generateTestData(singleFile: Boolean): Unit = {
        val support: FileSupport = dataFormat.format match {
            case FileFormat.PARQUET => FileSupport.make(new ParquetSupport(Benchmark.AVROSchema(numCols), dataCompression))
            case FileFormat.CSV => FileSupport.make(new CsvSupport(Benchmark.primitiveSchema(numCols).asJava, Benchmark.primitiveHeader(numCols)))
            case _ => throw new RuntimeException("Cannot generate file: Path not ending with '.pq' or '.csv'")
        }

        if (singleFile) {
            println(s"Generating testdata for 1 partition, with $numRows rows. ${if (generate_random) "Using randomly generated data." else ""}")
            if (dataFormat.format == FileFormat.PARQUET)
                println(s"Using compression strategy=${dataCompression.name()}")
            support.write(dataPath, Benchmark.generateTempData(numRows, 0, numCols, random=generate_random))
        } else {
            val splitsize = numRows / partitions
            println(s"Generating testdata for $partitions partitions, each with $splitsize rows (last partition gets ${numRows % partitions} rows). ${if (generate_random) "Using randomly generated data." else ""}")
            if (dataFormat.format == FileFormat.PARQUET)
                println(s"Using compression strategy=${dataCompression.name()}")
            val e: ExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
            for (x <- 0 until partitions) {
                e.execute(() => {
                    val amount =
                        if (x != partitions - 1)
                            splitsize
                        else
                            splitsize + numRows % partitions
                    val start = x * splitsize
                    val width = numCols
//                    println(s"Slice $x: $start to ${start + amount} (size=$amount)")
                    support.write(s"$dataPath/$x.${dataFormat.toExtensionString}", Benchmark.generateTempData(amount, start, width, random=generate_random))

                })
            }
            e.shutdown()
            e.awaitTermination(30, TimeUnit.MINUTES)
        }
    }

    private def test_command_valid: Boolean = testing.nonEmpty && (testing.get =="*" || Try {testing.get.toInt}.isSuccess)


    /** Entrypoint of this callable. Here we parse arguments, perform benchmarks, write output */
    override def call(): Unit = {
        println(s"Running in Scala ${scala.util.Properties.versionString}, program version ${Benchmark.programVersion}. Directing output to location: '${System.getProperty("file")}' (use '-Dfile=<val>' to change)")

        // Check if testing parameter is numeric or *
        testing.foreach(value => {
            if (!test_command_valid) {
                println(s"Cannot perform tests with parallelization modifier '$value'. Please assign a numeric modifier, or '*'.")
                return
            }
            println(s"Performing tests on local machine, using parallelization modifier '$value'.")
        })
        // Check if chosen computeCols subset is compatible
        if (computeCols > 0) {
            if (computeCols > numCols) {
                println(s"Cannot compute using $computeCols columns, because we only have $numCols columns in our datafile.")
                return
            }
        } else {
            computeCols = numCols
        }

        if (dataMultiplier < 1) {
            println(s"DataMultiplier has to be at least 1!")
            return
        }
        if (targets.contains("ds") && computeCols != Benchmark.Contributor.num) {
            println(s"Cannot use computeCols with Dataset benchmark. Datasets require strongly typed objects, so we cannot set a variable number of columns! Current strongly typed object supports exactly ${Benchmark.Contributor.num} columns.")
            return
        }

        println(s"Generation cols: $numCols. Compute cols: $computeCols. Commands to run: $targets")

        if (generate_force) {
            if (FileSupport.is_filepath(dataPath))
                FileSupport.file_delete(dataPath)
            else
                FileSupport.dir_delete(dataPath)
            val checksumpath = FileSupport.get_parent_dir(dataPath)+ FileSupport.get_filename(dataPath)+".crc"
            FileSupport.file_delete(checksumpath)
        }

        if (FileSupport.file_exists(dataPath)) {
            println(s"Using existing file: $dataPath")
        } else if (FileSupport.dir_exists(Benchmark.makeDataGenerationPath(dataPath, numRows, dataFormat, dataCompression, partitions, generate_random))) {
            dataPath = Benchmark.makeDataGenerationPath(dataPath, numRows, dataFormat, dataCompression, partitions, generate_random)
            println(s"Using existing directory: $dataPath")
        } else {
            if (FileSupport.is_filepath(dataPath)) {
                println(s"Generating file at path: $dataPath")
                generateTestData(true)
            } else {
                dataPath = Benchmark.makeDataGenerationPath(dataPath, numRows, dataFormat, dataCompression, partitions, generate_random)
                println(s"Generating files at path: $dataPath")
                generateTestData(false)
            }
            println(s"File generation complete!")
        }

        // Map keeps track of the amount of logs we have written. That way, we can quickly write a new one without having to worry about overriding
        val logs = scala.collection.mutable.Map[String, Long]()
        for (target: String <- targets.toArray(new Array[String](targets.size()))) {
            // Find method specified by the user
            val mtd = this.getClass.getMethod(target)

            // Prepare log location
            val lognr: Long = logs.get(target) match {
                case Some(value) => logs.update(target, value+1); value+1
                case None => logs += ((target, 0)); 0
            }

            Try {
                // Execute user-specified method
                mtd.invoke(this)
                println(s"Successfully executed target '$target'")
            }.recover { case e =>
                println(s"Error while executing target '$target'")
                println(e.printStackTrace())
                scala.sys.exit(1)
            }
        }
    }

    def rdd(): Unit= {
        val name =
            if (exclusiveExec.onlyArrow)
                "rddArrow"
            else if (exclusiveExec.onlySpark)
                "rddSpark"
            else
                "rdd"
        val session: SparkSession = getSession(name, testing)

        for(x: Int <- 0 until repeats) {
            if(!exclusiveExec.onlySpark)
                Benchmark.rddArrow(session, dataPath, dataFormat, computeCols, compute, partitions, dataMultiplier)
            if (!exclusiveExec.onlyArrow)
                Benchmark.rddSpark(session, dataPath, dataFormat, numCols, computeCols, compute, partitions)
        }

        session.close()
    }

    def df(): Unit= {
        val name =
            if (exclusiveExec.onlyArrow)
                "dfArrow"
            else if (exclusiveExec.onlySpark)
                "dfSpark"
            else
                "df"
        val session: SparkSession = getSession(name, testing)

        for(x: Int <- 0 until repeats) {
            if(!exclusiveExec.onlySpark)
                Benchmark.dfArrow(session, dataPath, dataFormat, computeCols, compute, partitions)
            if (!exclusiveExec.onlyArrow)
                Benchmark.dfSpark(session, dataPath, dataFormat, computeCols, compute, numCols, partitions)
        }

        session.close()
    }

    def df_sql(): Unit= {
        val name =
            if (exclusiveExec.onlyArrow)
                "dfSQLArrow"
            else if (exclusiveExec.onlySpark)
                "dfSQLSpark"
            else
                "dfSQL"
        val session: SparkSession = getSession(name, testing)

        for(x: Int <- 0 until repeats) {
            if(!exclusiveExec.onlySpark)
                Benchmark.dfArrowSQL(session, dataPath, dataFormat, computeCols, compute, partitions)
            if (!exclusiveExec.onlyArrow)
                Benchmark.dfSparkSQL(session, dataPath, dataFormat, computeCols, compute, numCols, partitions)
        }

        session.close()
    }

    def ds(): Unit= {
        val name =
            if (exclusiveExec.onlyArrow)
                "dsArrow"
            else if (exclusiveExec.onlySpark)
                "dsSpark"
            else
                "ds"
        val session: SparkSession = getSession(name, testing)

        for(x: Int <- 0 until repeats) {
            if(!exclusiveExec.onlySpark)
                Benchmark.dsArrow(session, dataPath, dataFormat, computeCols, compute, partitions)
            if (!exclusiveExec.onlyArrow)
                Benchmark.dsSpark(session, dataPath, dataFormat, computeCols, compute, numCols, partitions)
        }

        session.close()
    }
}