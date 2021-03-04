package org.arrowspark.spark.rdd

import org.apache.arrow.dataset.jni.NativeDataset
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.util.ArrowDatasetUtil
import org.apache.spark.sql.util.conversion.SchemaConversionUtil
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.arrow.dataset.provider.ArrowDatasetProvider
import org.arrowspark.spark.config.ArrowRDDReadConfig
import org.arrowspark.spark.rdd.partition.ArrowPartition
import org.arrowspark.util.RowWrapper

import scala.collection.JavaConverters.asScalaIteratorConverter


class ArrowRDD(@transient val sparkSession: SparkSession, private val datasetProvider: Broadcast[ArrowDatasetProvider], private val config: Broadcast[ArrowRDDReadConfig]) extends RDD[RowWrapper](sparkSession.sparkContext, Nil) with Logging {
    @transient protected val sc: SparkContext = sparkSession.sparkContext

    @transient protected lazy val dataset: NativeDataset = datasetProvider.value.provide(config.value)

    override def compute(split: Partition, context: TaskContext): Iterator[RowWrapper] = {
        val partition = split.asInstanceOf[ArrowPartition]
        val dataset = partition.openDataset(config.value).getOrElse(datasetProvider.value.provide(config.value))

        val sparkSchema = SchemaConversionUtil.fromArrowSchema(ArrowDatasetUtil.readSchema(dataset))
        val batchIterator = partition.batchIterator(dataset, sparkSchema, config.value.batchSize)

        val convertors: Array[Any => Any] = sparkSchema.map(field => CatalystTypeConverters.createToScalaConverter(field.dataType)).toArray
        batchIterator.toStream.flatMap(columnarBatch => columnarBatch.rowIterator().asScala).iterator.map(cbr => new RowWrapper(cbr, sparkSchema, convertors))
    }

    override protected def getPreferredLocations(split: Partition): Seq[String] = split.asInstanceOf[ArrowPartition].getPreferredLocations

    /**
     * Method to get partitions from user-selected partitioner.
     *
     * @return Generated partitions
     */
    override protected def getPartitions: Array[Partition] = {
        val conf = config.value
        try {
            val dataset: NativeDataset = datasetProvider.value.provide(config.value)
            val partitions = conf.partitioner.partitions(dataset, conf, sc.getConf)
            partitions.asInstanceOf[Array[Partition]]
        } catch {
            case t: Throwable =>
                logError(
                    s"""
                       |-----------------------------
                       |WARNING: Partitioning failed.
                       |-----------------------------
                       |Partitioning using the '${conf.partitioner.getClass.getSimpleName}' failed.
                       |
                       |Please check the stacktrace to determine the cause of the failure or check the Partitioner API documentation.
                       |Note: Not all partitioners are suitable for all toplogies and not all partitioners support views.
                       |-----------------------------
                       |""".stripMargin
                )
                throw t
        } finally {
//            println("\n----------------------End of partitioning --------------")
        }
    }
}
