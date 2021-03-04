package org.arrowspark.spark.rdd.partition.file

import org.apache.arrow.dataset.jni.NativeDataset
import org.apache.arrow.dataset.scanner.RegularScanOptions
import org.apache.arrow.memory.MemoryUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowDatasetUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.arrowspark.arrow.dataset.DatasetFileFormat
import org.arrowspark.arrow.dataset.file.RandomAccessFileDatasetFactory
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.partition.ArrowPartition
import org.arrowspark.util.TryUsing

import scala.collection.JavaConverters._

/**
 * Partition type to use with [[org.arrowspark.spark.rdd.partitioner.file.FilePartitioner]]
 *
 * @since 1.0
 */
object FilePartition {

    /**
     * Create a FilePartition with no preferred locations
     * @param index The partition's index within its parent RDD
     * @return the FilePartition
     */
    def apply(index: Int, filepath: String): FilePartition = new FilePartition(index, filepath, Nil)

    /**
     * Create a FilePartition from the Java API
     * @param index The partition's index within its parent RDD
     * @param locations The preferred locations (hostnames) for the data
     * @return the FilePartition
     */
    def create(index: Int, filepath: String, locations: java.util.List[String]): FilePartition =
        new FilePartition(index, filepath, locations.asScala)
}

/**
 * Object to store partition boundaries computed with [[org.arrowspark.spark.rdd.partitioner.file.FilePartitioner]]
 *
 * @param index The partition's index within its parent RDD
 * @param locations The preferred locations (hostnames) for the data
 * @since 1.0
 */
case class FilePartition(override val index: Int, filepath: String, locations: Seq[String]) extends ArrowPartition {
    override def equals(other: Any): Boolean = other match {
        case p: FilePartition => index.equals(p.index)
        case _ => false
    }

    override def toString: String = s"${getClass.getSimpleName}(index=$index, filepath=$filepath, locations=${locations.toList})"

    /**
     * Get a specified batch of results
     * @param dataset Dataset to get data from
     */
    override def batchIterator(dataset: NativeDataset, dataSchema: StructType, batchSize: Long): Iterator[ColumnarBatch] = {
        val scanner = dataset.newScan(RegularScanOptions(batchSize))
        ArrowDatasetUtil.readFromScanner(scanner, dataSchema, dataset=Option(dataset))
    }

    /** Optionally opens a new Arrow dataset. Default opening mechanism will be used if this function returns `None` */
    override def openDataset(config: ArrowRDDReadConfig): Option[NativeDataset] = {
        val format = DatasetFileFormat.fromPathLike(filepath)
        val path = if (config.dataSource.isFileURI)
            filepath.substring(7)
        else
            filepath
        TryUsing.autoCloseTry(new RandomAccessFileDatasetFactory(MemoryUtil.getAllocator, format, path))(factory => factory.finish()).toOption
    }
}
