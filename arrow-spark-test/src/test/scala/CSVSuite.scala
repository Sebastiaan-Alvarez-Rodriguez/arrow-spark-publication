import org.apache.arrow.dataset.file.FileFormat
import org.apache.spark.sql.execution.datasources.arrow.ArrowDataframeReader
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.arrowspark.arrow.dataset.DatasetFileFormat
import org.arrowspark.file.{BaseSupport, CsvSupport, FileSupport}
import org.arrowspark.spark.ArrowSpark
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.ArrowRDD
import org.arrowspark.spark.rdd.partitioner.file.FilePartitioner


/** Testing suite for CSV reading with our connector */
@RunWith(classOf[JUnitRunner])
class CSVSuite extends FunSuite with BeforeAndAfter {

    val numRows: Long = 1000

    /** If set, we always generate data before execution. Otherwise, we only do that when the data does not exist yet */
    val generate_force: Boolean = false
    val partitions: Int = 8

    val path: String = s"/tmp/csvtest/$numRows/$partitions/csv/"
    val isfilepath: Boolean = path.endsWith(".csv")

    var session: SparkSession = _

    def readConfig(session: SparkSession): ArrowRDDReadConfig = {
        // Create readConfig with URI to written Parquet file
        ArrowRDDReadConfig.builder()
            .withSparkConf(session.sparkContext.getConf)
            .withPartitioner(FilePartitioner)
            .withFileFormat(FileFormat.CSV)
            .withOption(s"${ArrowRDDReadConfig.datasetProviderOptionsProperty}.${ArrowRDDReadConfig.datasetProviderOptionsPathExtraProperty}", s"0.csv")
            .withNumPartitions(partitions)
            .withBatchSize(16*8*1024)
            .withDataSourceURI("file://"+path)
            .build()
    }

    before {
        val support = new CsvSupport(java.util.Arrays.asList(Class.forName("java.lang.Long"), Class.forName("java.lang.String")), "id,name").asInstanceOf[BaseSupport]
        if (isfilepath) {
            if (generate_force || !FileSupport.file_exists(path))
                ArrowSuiteHelper.generateTestData(support, DatasetFileFormat(FileFormat.CSV), path, partitions, numRows, singleFile = true)
        } else {
            if (generate_force || !FileSupport.dir_exists(path))
                ArrowSuiteHelper.generateTestData(support, DatasetFileFormat(FileFormat.CSV), path, partitions, numRows, singleFile = false)
        }
        session = SparkSession.builder.appName("arrow-spark").config("spark.master", "local[4]").getOrCreate

    }

    after {
        session.close()
        println("Experiment done!")
    }


    test("arrow-spark-rdd-csv") {
        val rdd: ArrowRDD = ArrowSpark.load(session.sparkContext, readConfig(session))

        assertEquals(numRows, rdd.count())

        val ans: Long = rdd.map(row => row.getLong(0)).reduce((a, b) => a+b)
        assertEquals(numRows*(numRows-1)/2, ans) // N-1+N-2...+1+0 = N*(N-1)/2
    }


    test("arrow-spark-df-csv") {
        val df: DataFrame = session.read.arrow(path)

        assertEquals(numRows, df.count())
        assertEquals(2, df.columns.length)

        val localval: Long = numRows
        val ans: Long = df.filter(row => row.getLong(0) < (localval/2)).count()
        assertEquals(numRows/2, ans)
    }

    test("arrow-spark-df_sql-csv") {
        val df: DataFrame = session.read.arrow(path)

        assertEquals(numRows, df.count())
        assertEquals(2, df.columns.length)

        df.createOrReplaceTempView("mytable")
        val ans = session.sql("SELECT sum(id) FROM mytable").first.get(0).asInstanceOf[Long]
        assertEquals(ans, numRows*(numRows-1)/2)
    }

    test("arrow-spark-ds-csv") {
        val ds: Dataset[Contributor] = session.read.arrow(path).as[Contributor](Encoders.product[Contributor])

        assertEquals(numRows, ds.count())
        assertEquals(2, ds.columns.length)

        val ans: Long = ds.agg(sum("id")).first.get(0).asInstanceOf[Long]
        assertEquals(ans, numRows*(numRows-1)/2)
    }


}
