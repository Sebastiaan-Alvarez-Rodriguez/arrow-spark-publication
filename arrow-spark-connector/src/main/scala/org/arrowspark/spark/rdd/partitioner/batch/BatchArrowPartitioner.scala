package org.arrowspark.spark.rdd.partitioner.batch

import org.apache.arrow.dataset.jni.NativeDataset
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.partition.ArrowPartition
import org.arrowspark.spark.rdd.partition.batch.BatchArrowPartition
import org.arrowspark.spark.rdd.partitioner.ArrowPartitioner

class BatchArrowPartitioner extends ArrowPartitioner {
    /**
     * Make partitions by assigning each partition an ArrowRecordBatch.
     *
     * @param dataset Arrow Dataset connector, which has access to the data that should be split into partitions
     * @param config  [[ArrowRDDReadConfig]] containing readBufferLength property
     * @return the partitions
     */
    override def partitions(dataset: NativeDataset, config: ArrowRDDReadConfig, numPartitions: Int): Array[ArrowPartition] = {
        (0 until numPartitions).map(x => BatchArrowPartition(x, x, numPartitions)).toArray.asInstanceOf[Array[ArrowPartition]]
    }
}

case object BatchArrowPartitioner extends BatchArrowPartitioner
