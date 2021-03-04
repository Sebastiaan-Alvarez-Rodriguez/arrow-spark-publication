package org.arrowspark.spark.rdd.partition.batch

import org.apache.arrow.dataset.jni.NativeDataset
import org.apache.arrow.dataset.scanner.RegularScanOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowDatasetUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.arrowspark.spark.rdd.partition.ArrowPartition
import org.arrowspark.util.FastSkipIterator

import scala.collection.JavaConverters.asScalaBufferConverter


/**
 * Partition type to use with [[org.arrowspark.spark.rdd.partitioner.batch.BatchArrowPrePartitioner]]
 *
 * @since 1.0
 */
object BatchArrowPrePartition {

    /**
     * Create a BatchArrowPrePartition with no preferred locations
     * @param index The partition's index within its parent RDD
     * @param indexStart The First batch to read
     * @param numBatches Number of batches we should read
     * @return the [[BatchArrowPrePartition]]
     */
    def apply(index: Int, indexStart: Long, numBatches: Long) = new BatchArrowPrePartition(index, indexStart, numBatches, Nil)


    /**
     * Create a BatchArrowPrePartition from the Java API
     * @param index The partition's index within its parent RDD
     * @param indexStart The First batch to read
     * @param numBatches Number of batches we should read
     * @param locations The preferred locations (hostnames) for the data
     * @return the [[BatchArrowPrePartition]]
     */
    def create(index: Int, indexStart: Long, numBatches: Long, locations: java.util.List[String]): BatchArrowPrePartition =
        new BatchArrowPrePartition(index, indexStart, numBatches, locations.asScala)
}

/**
 * Object to store partition boundaries computed with [[org.arrowspark.spark.rdd.partitioner.batch.BatchArrowPrePartitioner]]
 *
 * @param index Partition's index within its parent RDD
 * @param indexStart The First batch to read
 * @param numBatches Number of batches we should read
 * @param locations Preferred locations (hostnames) for the data
 * @since 1.0
 */
case class BatchArrowPrePartition(index: Int, indexStart: Long, numBatches: Long, locations: Seq[String]) extends ArrowPartition {
    override def hashCode(): Int = super.hashCode()

    override def equals(other: Any): Boolean = other match {
        case p: BatchArrowPrePartition if index.equals(p.index) && numBatches.equals(p.numBatches) && locations.equals(p.locations) => true
        case _ => false
    }

    override def toString: String = s"${getClass.getSimpleName}(index=$index, numBatches=$numBatches, locations=${locations.toList})"

    /**
     * Get a specified batch of results
     * @param dataset Dataset to get data from
     */
    override def batchIterator(dataset: NativeDataset, dataSchema: StructType, batchSize: Long): Iterator[ColumnarBatch] = {
        val scanner = dataset.newScan(RegularScanOptions(batchSize))
        val itr = ArrowDatasetUtil.readFromScanner(scanner, dataSchema, dataset=Option(dataset))
        new FastSkipIterator[ColumnarBatch](itr, indexStart, 1, (t: ColumnarBatch) => t.close()).zipWithIndex.filter(tuple => tuple._2 < numBatches).map(tuple => tuple._1)
    }
}
