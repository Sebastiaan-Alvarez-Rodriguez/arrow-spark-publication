/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.arrow

import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.arrow.ArrowFileFormat.UnsafeItr
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{ArrowDatasetUtil, ArrowFileFormatHelper, ArrowOptions}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.net.URLDecoder

object ArrowFileFormat {
    class UnsafeItr[T](delegate: Iterator[ColumnarBatch]) extends Iterator[ColumnarBatch] {
        val holder = new ColumnarBatchRetainer()

        override def hasNext: Boolean = {
            holder.release()
            delegate.hasNext
        }

        override def next(): ColumnarBatch = {
            val b = delegate.next()
            holder.retain(b)
            b
        }
    }

    class ColumnarBatchRetainer {
        private var retained: Option[ColumnarBatch] = None

        def retain(batch: ColumnarBatch): Unit = {
            if (retained.isDefined)
                throw new IllegalStateException
            retained = Some(batch)
        }

        def release(): Unit = {
            retained.foreach(b => b.close())
            retained = None
        }
    }
}

/** Custom fileformat implementation to read data through the C++ Arrow Dataset API */
class ArrowFileFormat extends FileFormat with DataSourceRegister with Serializable with Logging {
    /** Checks whether we can split the file */
    override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = false//ArrowDatasetUtil.isSplittable(ArrowOptions(sparkSession.sparkContext.getConf, options, path))

    /** Fetches the schema from Arrow */
    override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = ArrowDatasetUtil.readSchema(files, ArrowOptions(sparkSession.sparkContext.getConf, options, files.head.getPath))

    /** We do not support writing atm */
    override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = throw new UnsupportedOperationException("Write is not supported for Arrow sources")

    /** Determines whether we support returning batches of rows at a time. In principle, this is the case iff we deal with only atomic datatypes */
    override def supportBatch(session: SparkSession, schema: StructType): Boolean = ArrowFileFormatHelper.supportBatch(session, schema)

    /** Provides a mapping from file to data */
    override def buildReaderWithPartitionValues(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType, requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String], hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
        val taskBuildStart = System.nanoTime()
        val sqlConf = sparkSession.sessionState.conf
        val batchSize = sqlConf.parquetVectorizedReaderBatchSize

        val arrowOptions = ArrowOptions(sparkSession.sparkContext.getConf, options)
        (file: PartitionedFile) => {
            val splitable = ArrowDatasetUtil.isSplitable(arrowOptions.interpretedOrDefaultPathFileFormat(file.filePath))
            val file_start = if (splitable) file.start else -1
            val file_length = if (splitable) file.length else -1

            val factory = ArrowDatasetUtil.prepareRandomAccessFileRead(URLDecoder.decode(file.filePath, "UTF-8"), file_start, file_length, arrowOptions)
            val dataset = factory.finish()

            val filter = org.apache.arrow.dataset.filter.Filter.EMPTY
//            if (enableFilterPushDown) {
//                ArrowFilters.translateFilters(filters)
//            } else {
//                org.apache.arrow.dataset.filter.Filter.EMPTY
//            }

            val scanOptions = new ScanOptions(requiredSchema.map(f => f.name).toArray, filter, batchSize)
            val scanner = dataset.newScan(scanOptions)

            val columnarBatchItr: Iterator[ColumnarBatch] = ArrowDatasetUtil.readPartitionedFromScanner(scanner, requiredSchema, file.partitionValues, partitionSchema, arrowOptions, Option(dataset), Option(factory)).asInstanceOf[Iterator[ColumnarBatch]]

            new UnsafeItr(columnarBatchItr).asInstanceOf[Iterator[InternalRow]]
        }
    }

    override def shortName(): String = "arrow"

    override def toString: String = "Arrow"

    override def hashCode(): Int = getClass.hashCode()

    override def equals(other: Any): Boolean = other.isInstanceOf[ArrowFileFormat]
}
