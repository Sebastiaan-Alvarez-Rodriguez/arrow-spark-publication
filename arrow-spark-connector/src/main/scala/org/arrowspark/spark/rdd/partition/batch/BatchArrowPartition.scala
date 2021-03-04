package org.arrowspark.spark.rdd.partition.batch

import org.apache.arrow.dataset.jni.NativeDataset
import org.apache.arrow.dataset.scanner.RegularScanOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowDatasetUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.arrowspark.spark.rdd.partition.ArrowPartition
import org.arrowspark.util.FastSkipIterator

import scala.collection.JavaConverters.asScalaBufferConverter


/** Partition type to use with [[org.arrowspark.spark.rdd.partitioner.batch.BatchArrowPartitioner]] */
object BatchArrowPartition {

    /**
     * Create a BatchArrowPartition with no preferred locations
     * @param index The partition's index within its parent RDD
     * @param indexStart The First batch to read
     * @param jumpBatches Number of batches we should skip until we read the next (e.g. if indexStart=0 and jumpBatches=4, we read batch 0, 4, 8...)
     * @return the [[BatchArrowPartition]]
     */
    def apply(index: Int, indexStart: Long, jumpBatches: Long): BatchArrowPartition = new BatchArrowPartition(index, indexStart, jumpBatches, Nil)


    /**
     * Create a BatchArrowPartition from the Java API
     * @param index The partition's index within its parent RDD
     * @param indexStart The First batch to read
     * @param jumpBatches Number of batches we should skip until we read the next (e.g. if indexStart=0 and jumpBatches=4, we read batch 0, 4, 8...)
     * @param locations The preferred locations (hostnames) for the data
     * @return the [[BatchArrowPartition]]
     */
    def create(index: Int, indexStart: Long, jumpBatches: Long, locations: java.util.List[String]): BatchArrowPartition =
        new BatchArrowPartition(index, indexStart, jumpBatches, locations.asScala)
}

/**
 * Object to store partition boundaries computed with [[org.arrowspark.spark.rdd.partitioner.batch.BatchArrowPartitioner]]
 *
 * @param index Partition's index within its parent RDD
 * @param indexStart The First batch to read
 * @param jumpBatches Number of batches we should skip until we read the next (e.g. if indexStart=0 and jumpBatches=4, we read batch 0, 4, 8...)
 * @param locations Preferred locations (hostnames) for the data
 * @since 1.0
 */
case class BatchArrowPartition(index: Int, indexStart: Long, jumpBatches: Long, locations: Seq[String]) extends ArrowPartition {
    override def hashCode(): Int = super.hashCode()

    override def equals(other: Any): Boolean = other match {
        case p: BatchArrowPartition if index.equals(p.index) && jumpBatches.equals(p.jumpBatches) && locations.equals(p.locations) => true
        case _ => false
    }

    override def toString: String = s"${getClass.getSimpleName}(index=$index, jumpBatches=$jumpBatches, locations=${locations.toList})"

    /**
     * Get a specified batch of results
     * @param dataset Dataset to get data from
     */
    override def batchIterator(dataset: NativeDataset, dataSchema: StructType, batchSize: Long): Iterator[ColumnarBatch] = {
        val scanner = dataset.newScan(RegularScanOptions(batchSize))
        val itr = ArrowDatasetUtil.readFromScanner(scanner, dataSchema, dataset=Option(dataset))

        new FastSkipIterator[ColumnarBatch](itr, indexStart, jumpBatches, (t: ColumnarBatch) => t.close())
    }
}
