package org.arrowspark.spark.rdd.partitioner

import org.apache.arrow.dataset.jni.NativeDataset
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.partition.ArrowPartition
import org.arrowspark.spark.rdd.partitioner.batch.BatchArrowPartitioner

/**
 * The default collection partitioner implementation.
 * Wraps the [[BatchArrowPartitioner]] to use as default partitioner.
 *
 * @since 1.0
 */
class ArrowDefaultPartitioner extends ArrowPartitioner {

    override def partitions(dataset: NativeDataset, config: ArrowRDDReadConfig, numPartitions: Int): Array[ArrowPartition] = {
        BatchArrowPartitioner.partitions(dataset, config, numPartitions)
    }
}

case object ArrowDefaultPartitioner extends ArrowDefaultPartitioner
