import org.apache.arrow.dataset.file.FileFormat
import org.apache.spark.sql.execution.datasources.arrow.ArrowDataframeReader
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.arrowspark.arrow.dataset.DatasetFileFormat
import org.arrowspark.file.{BaseSupport, FileSupport, ParquetSupport}
import org.arrowspark.spark.ArrowSpark
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.ArrowRDD
import org.arrowspark.spark.rdd.partitioner.file.FilePartitioner

@RunWith(classOf[JUnitRunner])
class ParquetSuite extends FunSuite with BeforeAndAfter {

    /** Amount of rows we generate */
    val numRows: Long = 1000

    /** If set, we always generate data before execution. Otherwise, we only generate data if it does not exist yet  */
    val generate_force: Boolean = false
    val partitions: Int = 8
    var session: SparkSession = _

    def readConfig(session: SparkSession): ArrowRDDReadConfig = {
        ArrowRDDReadConfig.builder()
            .withSparkConf(session.sparkContext.getConf)
            .withPartitioner(FilePartitioner)
            .withFileFormat(FileFormat.PARQUET)
            .withOption(s"${ArrowRDDReadConfig.datasetProviderOptionsProperty}.${ArrowRDDReadConfig.datasetProviderOptionsPathExtraProperty}", s"0.pq")
            .withNumPartitions(partitions)
            .withBatchSize(16*8*1024)
            .withDataSourceURI("file://"+path)
            .build()
    }

    /** Location of generated parquet file */
    var path: String = s"/tmp/pqtest/$numRows/$partitions/pq/"
    val isfilepath: Boolean = path.endsWith(".pq")

    before {
        if (isfilepath) {
            if (generate_force || !FileSupport.file_exists(path))
                ArrowSuiteHelper.generateTestData(new ParquetSupport(schema).asInstanceOf[BaseSupport], DatasetFileFormat(FileFormat.PARQUET), path, partitions, numRows, singleFile = true)
        } else {
            if (generate_force || !FileSupport.dir_exists(path))
                ArrowSuiteHelper.generateTestData(new ParquetSupport(schema).asInstanceOf[BaseSupport], DatasetFileFormat(FileFormat.PARQUET), path, partitions, numRows, singleFile = false)
        }
        session = SparkSession.builder.appName("arrow-spark").config("spark.master", "local[4]").getOrCreate
        // Create readConfig with URI to written Parquet file
    }

    after {
        session.close()
    }



    def schema: String = {
        """{
          | "namespace": "org.arrowspark.arrow-spark-test",
          | "type": "record",
          | "name": "User",
          | "fields": [
          |     {"name": "id", "type": ["long", "null"]},
          |     {"name": "name", "type": ["string", "null"]}
          | ]
          |}""".stripMargin
    }


//    case class Contributor(id: Long, name: String)

    test("arrow-spark-rdd-pq") {
        val rdd: ArrowRDD = ArrowSpark.load(session.sparkContext, readConfig(session))

        assertEquals(numRows, rdd.count())

        val ans: Long = rdd.map(row => row.getString(1).substring(1).toLong).reduce((a, b) => a+b)
        assertEquals(numRows*(numRows-1)/2, ans) // N-1+N-2...+1+0 = N*(N-1)/2
    }

    test("arrow-spark-df-pq") {
        val df: DataFrame = session.read.arrow(path)

        assertEquals(numRows, df.count())
        assertEquals(2, df.columns.length)

        val localval: Long = numRows
        val ans: Long = df.filter(row => row.getLong(0) < (localval/2)).count()
        assertEquals(numRows/2, ans)
    }

    test("arrow-spark-df_sql-pq") {
        val df: DataFrame = session.read.arrow(path)

        assertEquals(numRows, df.count())
        assertEquals(2, df.columns.length)

        df.createOrReplaceTempView("mytable")
        val ans = session.sql("SELECT sum(id) FROM mytable").first.get(0).asInstanceOf[Long]
        assertEquals(ans, numRows*(numRows-1)/2)
    }

    test("arrow-spark-ds-pq") {
        val ds: Dataset[Contributor] = session.read.format("org.apache.spark.sql.execution.datasources.arrow.ArrowFileFormat").load(path).as[Contributor](Encoders.product[Contributor])

        assertEquals(numRows, ds.count())
        assertEquals(2, ds.columns.length)

        val ans: Long = ds.agg(sum("id")).first.get(0).asInstanceOf[Long]
        assertEquals(ans, numRows*(numRows-1)/2)
    }
}
