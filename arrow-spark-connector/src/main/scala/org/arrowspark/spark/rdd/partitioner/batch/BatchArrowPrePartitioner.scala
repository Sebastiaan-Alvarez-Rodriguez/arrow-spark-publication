package org.arrowspark.spark.rdd.partitioner.batch

import org.apache.arrow.dataset.jni.NativeDataset
import org.apache.arrow.dataset.scanner.RegularScanOptions
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.partition.ArrowPartition
import org.arrowspark.spark.rdd.partition.batch.BatchArrowPrePartition
import org.arrowspark.spark.rdd.partitioner.ArrowPartitioner

import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}
import scala.collection.mutable.ArrayBuffer

@deprecated("Too inefficient partitioner to read data", "2021-01-26")
class BatchArrowPrePartitioner extends ArrowPartitioner {

    protected def doPartition(numExecutors: Int, numbatches: Long, batchsize: Long): Array[BatchArrowPrePartition] = {
        val batchesPerExecutor = numbatches / numExecutors // rounding down
        val partitions: ArrayBuffer[BatchArrowPrePartition] = new ArrayBuffer[BatchArrowPrePartition]

        val exactfit = batchesPerExecutor * numExecutors == numbatches
        val repeats = if (exactfit) numExecutors else numExecutors - 1
        for (x <- 0 until repeats)
            partitions += BatchArrowPrePartition(x, x * batchesPerExecutor, batchesPerExecutor)
        if (!exactfit)
            partitions += BatchArrowPrePartition(numExecutors - 1, (numExecutors - 1) * batchesPerExecutor, numbatches - batchesPerExecutor * (numExecutors - 1))
        partitions.toArray
    }

    /**
     * Make partitions by assigning each partition 1 ArrowRecordBatch.
     *
     * @param dataset Arrow Dataset connector, which has access to the data that should be split into partitions
     * @param config  [[ArrowRDDReadConfig]] containing readBufferLength property
     * @return the partitions
     */
    //noinspection ScalaDeprecation
    override def partitions(dataset: NativeDataset, config: ArrowRDDReadConfig, numPartitions: Int): Array[ArrowPartition] = {
        var batchsize: Long = config.batchSize
        var numbatches: Long = num_batches(dataset, batchsize)

        while (numPartitions > numbatches) {
            if (batchsize < 100) { // It is futile to reduce the batchsize to get a higher number of batches...
                return doPartition(numbatches.asInstanceOf[Int], numbatches, batchsize).asInstanceOf[Array[ArrowPartition]]
            }
            val relation: Float = numPartitions.toFloat / numbatches.toFloat //e.g. 20 / 4 = 5 (5 times too many partitions for the amount of batches
            batchsize /= (relation + 1).toInt
            numbatches = num_batches(dataset, batchsize)
        }
        doPartition(numPartitions, numbatches, batchsize).asInstanceOf[Array[ArrowPartition]]
    }

     @deprecated("Method is too expensive, reads entire dataset", "2021-01-26")
    private def num_batches(dataset: NativeDataset, batchSize: Long): Long = {
        val scanner = dataset.newScan(RegularScanOptions(batchSize))
        val num: Long = scanner.scan.asScala.toStream.flatMap(scantask => scantask.scan.asScala).map(_ => 1L).sum
        scanner.close()
        num
    }
}

//noinspection ScalaDeprecation
case object BatchArrowPrePartitioner extends BatchArrowPrePartitioner
