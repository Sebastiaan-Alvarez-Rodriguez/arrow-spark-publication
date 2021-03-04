package org.arrowspark.spark.rdd.partitioner

import org.apache.arrow.dataset.jni.NativeDataset
import org.apache.spark.SparkConf
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.partition.ArrowPartition
import org.arrowspark.spark.util.SparkUtil

/**
 * ArrowPartitioner provides the partitions of a collection, backed by Arrow
 * @since 1.0
 */
abstract class ArrowPartitioner extends Serializable {

    /** Returns a sensible amount of partitions for the given amount of executors */
    def getNumPartitions(numExecutors: Int): Int = numExecutors * 4
    /**
     * Generate the Partitions
 *
     * @param dataset Arrow Dataset uplink to data that should be split into partitions
     * @param config  the [[ArrowRDDReadConfig]]
     * @return the partitions
     */
    def partitions(dataset: NativeDataset, config: ArrowRDDReadConfig, sparkConf: SparkConf): Array[ArrowPartition] = {
        val numpartitions =
            if (config.partitionerOptions.contains(ArrowRDDReadConfig.partitionerOptionsNumPartitionsProperty))
                config.partitionerOptions(ArrowRDDReadConfig.partitionerOptionsNumPartitionsProperty).toInt
            else
                getNumPartitions(SparkUtil.getNumExecutors(sparkConf))
        partitions(dataset, config, numpartitions)
    }

    /**
     * Generate the Partitions
 *
     * @param dataset       Arrow Dataset uplink to data that should be split into partitions
     * @param config        the [[ArrowRDDReadConfig]]
     * @param numPartitions the number of partitions we prefer to use.
     *
     *                      '''Note:''' partitioners may deviate from this number
     * @return the partitions
     */
    def partitions(dataset: NativeDataset, config: ArrowRDDReadConfig, numPartitions: Int): Array[ArrowPartition]
}
