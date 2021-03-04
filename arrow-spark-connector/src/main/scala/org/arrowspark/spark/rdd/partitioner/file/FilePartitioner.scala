package org.arrowspark.spark.rdd.partitioner.file

import org.apache.arrow.dataset.jni.NativeDataset
import org.arrowspark.arrow.dataset.DatasetFileFormat
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.partition.ArrowPartition
import org.arrowspark.spark.rdd.partition.file.FilePartition
import org.arrowspark.spark.rdd.partitioner.{ArrowDefaultPartitioner, ArrowPartitioner}

import scala.collection.mutable.ArrayBuffer

class FilePartitioner extends ArrowPartitioner {
    /**
     * Make partitions transparently by scanning [[ArrowRDDReadConfig.dataSource]].
     * If this is a directory, then we create as many partitions as there are files, ensuring 1 partition == 1 file.
     * If we point to a file instead, then we fall back to the [[ArrowDefaultPartitioner]] for that single file.
     *
     * In case we deal with a directory, we use [[ArrowRDDReadConfig.partitionerOptionsDataMultiplierProperty]] to determine how many hardlinks every file has.
     *
     * @param dataset Arrow Dataset connector, which has access to the data that should be split into partitions
     * @param config  the [[ArrowRDDReadConfig]]
     * @return the partitions
     */
    override def partitions(dataset: NativeDataset, config: ArrowRDDReadConfig, numPartitions: Int): Array[ArrowPartition] = {
        val data = config.dataSource
        if (!data.isFileURI)
            throw new RuntimeException(s"Cannot use ${getClass.getSimpleName} for non-filelike URIs. Does your datasource start with 'file://'?")

        if (data.isFileSingle)
            return ArrowDefaultPartitioner.partitions(dataset, config, numPartitions)

        val extension =
            if (config.datasetProviderOptions.contains(ArrowRDDReadConfig.datasetProviderOptionsFileTypeProperty))
                DatasetFileFormat.fromString(config.datasetProviderOptions(ArrowRDDReadConfig.datasetProviderOptionsFileTypeProperty)).toExtensionString
            else
                throw new RuntimeException("Cannot determine file type when passing something that is not a file URI. Consider supplying the type in the ArrowRDDReadConfig, using `readconfig.withFileFormat(format)`")

        val dataMultiplier: Int = config.datasetProviderOptions.getOrElse(ArrowRDDReadConfig.partitionerOptionsDataMultiplierProperty, "1").toInt

        val partitions: ArrayBuffer[FilePartition] = new ArrayBuffer[FilePartition](dataMultiplier*numPartitions)
        for (x <- 0 until numPartitions) {
            val p_base = x*dataMultiplier
            partitions += FilePartition(p_base, s"${config.dataSource.path}/$x.$extension")
            for (y <- 0 until dataMultiplier-1)
                partitions += FilePartition(p_base+y+1, s"${config.dataSource.path}/${x}_$y.$extension")
        }

        partitions.toArray.asInstanceOf[Array[ArrowPartition]]
    }
}

case object FilePartitioner extends FilePartitioner
