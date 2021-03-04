package org.arrowspark.spark.config

trait ArrowInputConfig extends ArrowBaseConfig {
    override val configPrefix: String = "arrow.spark.input."

    /**
     * The partition property
     *
     * Represents the name of the partitioner to use when partitioning the data in the collection.
     * Default: `ArrowDefaultPartitioner`
     */
    val partitionerProperty: String = "partitioner".toLowerCase

    /**
     * The partitioner options property
     *
     * Represents a map of options for customising the configuration of a partitioner.
     * Default: `Map.empty[String, String]`
     */
    val partitionerOptionsProperty: String = "partitionerOptions".toLowerCase

    /** Preferred number of partitions to generate */
    val partitionerOptionsNumPartitionsProperty: String = s"numPartitions".toLowerCase

    /** Used data multiplier (how many copies of the same file exist as hardlinks) */
    val partitionerOptionsDataMultiplierProperty: String = s"dataMultiplier".toLowerCase

    /**
     * The datasetProvider property
     *
     * Represents the name of the datasetProvider to use when the input data is required.
     * Default: `ArrowDefaultDatasetProvider`
     */
    val datasetProviderProperty: String = "datasetProvider".toLowerCase


    /** DatasetProvider options */
    val datasetProviderOptionsProperty: String = "datasetProvider".toLowerCase

    /** Format of read data */
    val datasetProviderOptionsFileTypeProperty: String = s"fileType".toLowerCase

    /**
     * Uri part to append when Arrow is about to read a directory instead of a file.
     *
     * '''Note:''' Use this property only when reading multiple files AND you know what you are doing.
     */
    val datasetProviderOptionsPathExtraProperty: String = s"URIextra".toLowerCase

    /**
     * The batch size property
     *
     * The size of batches used by Arrow. Smaller batch sizes will result in more batches in Arrow.
     */
    val batchSizeProperty: String = "batchSize".toLowerCase

}
