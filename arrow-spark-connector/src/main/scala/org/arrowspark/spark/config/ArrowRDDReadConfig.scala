package org.arrowspark.spark.config
import org.apache.arrow.dataset.file.FileFormat
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.arrowspark.arrow.dataset.DatasetFileFormat
import org.apache.arrow.dataset.provider.{ArrowDatasetProvider, ArrowDefaultDatasetProvider}
import org.arrowspark.spark.config
import org.arrowspark.spark.rdd.partitioner.{ArrowDefaultPartitioner, ArrowPartitioner}

import java.util
import javax.annotation.Nonnull
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.mutable
import scala.util.Try

case object ArrowRDDReadConfig extends ArrowInputConfig {
    protected val DefaultBatchSize: Long = 20480
    protected val DefaultDatasetProvider: ArrowDatasetProvider = ArrowDefaultDatasetProvider
    protected val DefaultDatasetProviderOptions = Map.empty[String, String]
    protected val DefaultPartitioner: ArrowPartitioner = ArrowDefaultPartitioner
    protected val DefaultPartitionerOptions = Map.empty[String, String]
    protected val DefaultPartitionerPath = "org.arrowspark.spark.rdd.partitioner."
    protected val DefaultDatasetProviderPath = "org.apache.arrow.dataset.provider."

    /** The type of the ArrowConfig */
    override type Self = ArrowRDDReadConfig

    override def apply(options: collection.Map[String, String], default: Option[ArrowRDDReadConfig]): ArrowRDDReadConfig = apply(options, default, None, None)

    def apply(options: collection.Map[String, String], default: Option[ArrowRDDReadConfig], partitioner: Option[ArrowPartitioner], provider: Option[ArrowDatasetProvider]): ArrowRDDReadConfig = {
        val cleanedOptions = stripPrefix(options)
        val defaultProvider = default.map(conf => conf.datasetProvider).getOrElse(DefaultDatasetProvider)
        val defaultPartitioner = default.map(conf => conf.partitioner).getOrElse(DefaultPartitioner)
        val defaultPartitionerOptions = default.map(conf => conf.partitionerOptions).getOrElse(DefaultPartitionerOptions)
        val partitionerOptions = defaultPartitionerOptions ++ getPartitionerOptions(cleanedOptions)

        val defaultDatasetProviderOptions = default.map(conf => conf.datasetProviderOptions).getOrElse(DefaultDatasetProviderOptions)
        val datasetProviderOptions = defaultDatasetProviderOptions ++ getDatasetProviderOptions(cleanedOptions)

        val dataURI: DataSource = default.map(conf => conf.dataSource).getOrElse(datasourceURI(cleanedOptions))
        new ArrowRDDReadConfig(
            batchSize = cleanedOptions.get(batchSizeProperty).map(_.toLong).orElse(default.map(conf => conf.batchSize)).getOrElse(DefaultBatchSize),
            dataSource = dataURI,
            datasetProvider = provider.getOrElse(cleanedOptions.get(datasetProviderProperty).map(getDatasetProvider).getOrElse(defaultProvider)),
            datasetProviderOptions = datasetProviderOptions,
            partitioner = partitioner.getOrElse(cleanedOptions.get(partitionerProperty).map(getPartitioner).getOrElse(defaultPartitioner)),
            partitionerOptions = partitionerOptions
        )
    }

    /**
     * Create a configuration easily from the Java API using the `JavaSparkContext`
     *
     * Uses the prefixed properties that are set in the Spark configuration to create the config.
     *
     * @see [[configPrefix]]
     * @param javaSparkContext the java spark context
     * @return the configuration
     */
    override def create(@Nonnull javaSparkContext: JavaSparkContext): config.ArrowRDDReadConfig = apply(javaSparkContext.getConf)

    /**
     * Create a configuration easily from the Java API using the `JavaSparkContext`
     *
     * Uses the prefixed properties that are set in the Spark configuration to create the config.
     *
     * @see [[configPrefix]]
     * @param sparkSession the SparkSession
     * @return the configuration
     */
    override def create(@Nonnull sparkSession: SparkSession): config.ArrowRDDReadConfig = apply(sparkSession)

    /**
     * Create a configuration easily from the Java API using the `sparkConf`
     *
     * Uses the prefixed properties that are set in the Spark configuration to create the config.
     *
     * @see [[configPrefix]]
     * @param sparkConf the spark configuration
     * @return the configuration
     */
    override def create(@Nonnull sparkConf: SparkConf): config.ArrowRDDReadConfig = apply(sparkConf)

    /**
     * Create a configuration from the `sparkConf`
     *
     * Uses the prefixed properties that are set in the Spark configuration to create the config.
     *
     * @see [[configPrefix]]
     * @param sparkConf the spark configuration
     * @param options   overloaded parameters
     * @return the configuration
     */
    override def create(@Nonnull sparkConf: SparkConf, options: util.Map[String, String]): config.ArrowRDDReadConfig = apply(sparkConf, options.asScala)

    /**
     * Create a configuration easily from the Java API using the values in the `Map`
     *
     * '''Note:''' Values in the map do not need to be prefixed with the [[configPrefix]].
     *
     * @param options a map of properties and their string values
     * @return the configuration
     */
    override def create(options: util.Map[String, String]): config.ArrowRDDReadConfig = apply(options.asScala)

    /**
     * Create a configuration easily from the Java API using the values in the `Map`, using the optional default configuration for any
     * default values.
     *
     * '''Note:''' Values in the map do not need to be prefixed with the [[configPrefix]].
     *
     * @param options a map of properties and their string values
     * @param default the optional default configuration, used for determining the default values for the properties
     * @return the configuration
     */
    override def create(options: util.Map[String, String], @Nonnull default: config.ArrowRDDReadConfig): config.ArrowRDDReadConfig = apply(options.asScala, Option(default))

    def builder(): Builder = new Builder

    class Builder {
        private var factory: Option[ArrowDatasetProvider] = None
        private var partitioner: Option[ArrowPartitioner] = None
        private var options: mutable.Map[String, String] = mutable.Map[String, String]()

        def withDataSourceURI(uri: String): Builder = {
            options += (s"$datasourceURIProperty" -> uri)
            this
        }

        def withDatasetProvider(factory: ArrowDatasetProvider): Builder = {
            this.factory = Option(factory)
            this
        }

        def withFileFormat(format: String): Builder = {
            options += (s"$datasetProviderOptionsProperty.$datasetProviderOptionsFileTypeProperty" -> format)
            this
        }
        def withFileFormat(format: DatasetFileFormat): Builder = withFileFormat(format.toString)

        def withFileFormat(format: FileFormat): Builder = withFileFormat(DatasetFileFormat(format))

        def withPartitioner(partitioner: ArrowPartitioner): Builder = {
            this.partitioner = Option(partitioner)
            this
        }

        def withNumPartitions(partitions: Int): Builder = {
            options += (s"$partitionerOptionsProperty.$partitionerOptionsNumPartitionsProperty" -> partitions.toString)
            this
        }

        def withBatchSize(batchSize: Long): Builder = {
            options += (batchSizeProperty -> batchSize.toString)
            this
        }
        def withBatchSize(batchSize: Option[Long]): Builder = batchSize match {
            case Some(value) => withBatchSize(value)
            case None => this
        }

        def withOption(key: String, value: String): Builder = {
            options += (key -> value)
            this
        }

        def withOptions(opts: Map[String, String]): Builder = {
            options ++= opts
            this
        }

        def withSparkConf(conf: SparkConf): Builder =
            withOptions(conf.getAll.toMap)

        def build(): ArrowRDDReadConfig = ArrowRDDReadConfig(options.toMap, None, partitioner, factory)
    }

    private def runtimeLoad(path: String): Object = {
        val loadedObject = Try({
            val clazz = Class.forName(path)
            if (path.endsWith("$")) {
                clazz.getField("MODULE$").get(clazz)
            } else {
                //noinspection ScalaDeprecation
                clazz.newInstance().asInstanceOf[Object]
            }
        })
        if (loadedObject.isFailure)
            throw new RuntimeException(s"Could not load object from path: '$path'. Please check whether the namespace is correct.")
        loadedObject.get
    }

    private def getDatasetProvider(datasetProvider: String): ArrowDatasetProvider = {
        val datasetProviderClassName = if (datasetProvider.contains(".")) datasetProvider else s"$DefaultDatasetProviderPath$datasetProvider"
        runtimeLoad(datasetProviderClassName).asInstanceOf[ArrowDatasetProvider]
    }

    private def getPartitioner(partitionerName: String): ArrowPartitioner = {
        val partitionerClassName = if (partitionerName.contains(".")) partitionerName else s"$DefaultPartitionerPath$partitionerName"
        runtimeLoad(partitionerClassName).asInstanceOf[ArrowPartitioner]
    }

    private def getPartitionerOptions(options: collection.Map[String, String]): collection.Map[String, String] = {
        filterMap(options, partitionerOptionsProperty)
    }

    private def getDatasetProviderOptions(options: collection.Map[String, String]): collection.Map[String, String] = {
        filterMap(options, datasetProviderOptionsProperty)
    }

    private def filterMap(map: collection.Map[String, String], keyPrefix: String): collection.Map[String, String] = {
        stripPrefix(map).map(kv => (kv._1.toLowerCase, kv._2)).filter(kv => kv._1.startsWith(keyPrefix)).map(kv => (kv._1.stripPrefix(s"$keyPrefix."), kv._2))
    }
}
case class ArrowRDDReadConfig(
                                 batchSize: Long = ArrowRDDReadConfig.DefaultBatchSize,
                                 dataSource: DataSource,
                                 datasetProvider: ArrowDatasetProvider,
                                 datasetProviderOptions: collection.Map[String, String],
                                 partitioner: ArrowPartitioner = ArrowRDDReadConfig.DefaultPartitioner,
                                 partitionerOptions: collection.Map[String, String] = ArrowRDDReadConfig.DefaultPartitionerOptions
) extends ArrowSourceConfig with ArrowMutableConfig {

    /**
     * Defines Self as a type that can be used to return a copy of the object i.e. a different instance of the same type
     */
    override type Self = ArrowRDDReadConfig

    /**
     * Creates a new config with the options applied
     *
     * @param key   the configuration key
     * @param value the configuration value
     * @return an updated config
     */
    override def withOption(key: String, value: String): ArrowRDDReadConfig = ArrowRDDReadConfig(this.asOptions + (key -> value))

    /**
     * Creates a new config with the options applied
     *
     * @param options a map of options to be applied to the config
     * @return an updated config
     */
    override def withOptions(options: collection.Map[String, String]): ArrowRDDReadConfig = ArrowRDDReadConfig(options, Some(this))

    /**
     * Creates a map of options representing this config.
     *
     * @return the map representing the config
     */
    override def asOptions: collection.Map[String, String] = {
        val options: mutable.Map[String, String] = mutable.Map(ArrowRDDReadConfig.partitionerProperty -> partitioner.getClass.getName)
        options += ArrowRDDReadConfig.datasourceURIProperty -> dataSource.path
        partitionerOptions.map(kv => options += s"${ArrowRDDReadConfig.partitionerOptionsProperty}.${kv._1}".toLowerCase -> kv._2)
        options += ArrowRDDReadConfig.batchSizeProperty -> batchSize.toString

        options.toMap
    }

    /**
     * Creates a new config with the options applied
     *
     * @param options a map of options to be applied to the config
     * @return an updated config
     */
    override def withOptions(options: util.Map[String, String]): ArrowRDDReadConfig = withOptions(options.asScala)

    /**
     * Creates a map of options representing the configuration
     *
     * @return the map representing the configuration values
     */
    override def asJavaOptions: util.Map[String, String] = asOptions.asJava
}
