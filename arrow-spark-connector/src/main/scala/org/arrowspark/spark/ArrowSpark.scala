package org.arrowspark.spark

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.arrow.dataset.provider.{ArrowDatasetProvider, ArrowDefaultDatasetProvider}
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.ArrowRDD

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * The ArrowSpark helper allows easy creation of RDDs, DataFrames or Datasets from Arrow.
 * @since 1.0
 */
object ArrowSpark {
    /**
     * Create a builder for configuring an [[ArrowRDD]]
     * @return an ArrowRDD Builder
     */
    def builder(): Builder = new Builder

    /**
     * Load data from Arrow
     * @param sc the Spark context containing the Arrow connection configuration
     * @return a ArrowRDD
     */
    def load(sc: SparkContext): ArrowRDD = load(sc, ArrowRDDReadConfig(sc))

    /**
     * Load data from Arrow
     * @param sc     Spark context
     * @param config the custom config to use for this prototype
     * @return a ArrowRDD
     * @since 1.0
     */

    def load(sc: SparkContext, config: ArrowRDDReadConfig): ArrowRDD = builder().setSparkContext(sc).setConfig(config).build().toRDD


    def save[D: ClassTag](rdd: RDD[D]): Unit = save(rdd, ArrowRDDReadConfig(rdd.sparkContext))

    /**
     * Save data to Arrow
     * @param rdd RDD data to save using Arrow backend
     * @param config custom config
     * @tparam D type of the data in the RDD
     * @since 1.0
     */
    def save[D: ClassTag](rdd: RDD[D], config: ArrowRDDReadConfig): Unit = throw new RuntimeException("ArrowRDD does not support saving")

//    /**
//     * Creates a DataFrameReader with `arrow` as the source
//     * @param sparkSession the SparkSession
//     * @return the DataFrameReader
//     */
//    def read(sparkSession: SparkSession): DataFrameReader = sparkSession.read.format("com.arrow.org.apache.spark.sql")

//    /**
//     * Creates a DataFrameWriter with the `arrow` underlying output data source.
//     * @param dataset the Dataset to convert into a DataFrameWriter
//     * @return the DataFrameWriter
//     */
//    def write[T](dataset: Dataset[T]): DataFrameWriter[T] = dataset.write.format("com.arrow.org.apache.spark.sql")


    /** Builder for configuring and creating a [[ArrowSpark]] */
    class Builder {
        private var sparkSession: Option[SparkSession] = None
        private var provider: Option[ArrowDatasetProvider] = None
        private var config: Option[ArrowRDDReadConfig] = None
        private var options: mutable.Map[String, String] = mutable.Map[String, String]()

        def build(): ArrowSpark = {
            require(sparkSession.isDefined, "The SparkSession must be set, either explicitly or via the SparkContext")

            val mergedConf = ArrowRDDReadConfig(options.toMap, config)
            new ArrowSpark(sparkSession.get, provider.getOrElse(config.map(conf => conf.datasetProvider).getOrElse(ArrowDefaultDatasetProvider)), mergedConf)
        }

        /**
         * Sets the SparkSession from the sparkContext
         * @param sparkSession for the RDD
         */
        def setSparkSession(sparkSession: SparkSession): Builder = {
            this.sparkSession = Option(sparkSession)
            this
        }

        /**
         * Sets the SparkSession from the sparkContext
         * @param sparkContext for the RDD
         */
        def setSparkContext(sparkContext: SparkContext): Builder = {
            this.sparkSession = Option(SparkSession.builder().config(sparkContext.getConf).getOrCreate())
            this
        }

        /**
         * Sets the SparkSession from the javaSparkContext
         * @param javaSparkContext for the RDD
         */
        def setJavaSparkContext(javaSparkContext: JavaSparkContext): Builder = setSparkContext(javaSparkContext.sc)

        /**
         * Append a configuration option
         * These options can be used to configure all aspects of how to connect to Arrow
         * @param key   the configuration key
         * @param value the configuration value
         */
        def setOption(key: String, value: String): Builder = {
            this.options += (key -> value)
            this
        }

        /**
         * Set configuration options
         * These options can configure all aspects of how to connect to Arrow
         * @param options the configuration options
         */
        def setOptions(options: Map[String, String]): Builder = {
            this.options ++= options
            this
        }

        /**
         * Set configuration options
         * These options can configure all aspects of how to connect to Arrow
         * @param options the configuration options
         */
        def setOptions(options: java.util.Map[String, String]): Builder = {
            this.options ++= options.asScala
            this
        }

        /**
         * Sets the [[ArrowDatasetProvider]] to use to construct the Arrow Dataset
         * @param provider the Arrow Dataset org.arrowspark.arrow.dataset we get our data from
         */
        def setDatasetProvider(provider: ArrowDatasetProvider): Builder = {
            this.provider = Option(provider)
            this
        }

        /**
         * Sets the [[ArrowRDDReadConfig]] to use
         *
         * @param config the ArrowRDDReadConfig
         */
        def setConfig(config: ArrowRDDReadConfig): Builder = {
            this.config = Option(config)
            this
        }
    }

}
/**
 * The ArrowSpark class
 *
 * '''Note:''' Creation of this class should be done with [[ArrowSpark#Builder]].
 * @since 1.0
 */
case class ArrowSpark(sparkSession: SparkSession, datasetProvider: ArrowDatasetProvider, config: ArrowRDDReadConfig) {

    private def rdd: ArrowRDD = new ArrowRDD(sparkSession, sparkSession.sparkContext.broadcast(datasetProvider), sparkSession.sparkContext.broadcast(config))

    /**
     * Creates a `RDD` for the collection
     * @return a ArrowRDD[D]
     */
    def toRDD: ArrowRDD = rdd
}

