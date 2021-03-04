package org.arrowspark.spark.rdd.partition

import org.apache.arrow.dataset.jni.NativeDataset
import org.apache.spark.Partition
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.arrowspark.spark.config.ArrowRDDReadConfig

trait ArrowPartition extends Partition {

    /**
     * Get the batch of results which we contain with this partition
     * @param dataset Dataset to get data from
     * @param dataSchema Schema of the data to read
     * @param batchSize Preferred size of each batch
     */
    def batchIterator(dataset: NativeDataset, dataSchema: StructType, batchSize: Long): Iterator[ColumnarBatch]

    def getPreferredLocations: Seq[String] = Nil

    /** Optionally opens a new Arrow dataset. Default opening mechanism will be used if this function returns `None` */
    def openDataset(config: ArrowRDDReadConfig): Option[NativeDataset] = None
}
