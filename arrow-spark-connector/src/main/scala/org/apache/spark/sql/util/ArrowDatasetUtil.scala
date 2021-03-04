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

package org.apache.spark.sql.util

import org.apache.arrow.dataset.file.FileFormat
import org.apache.arrow.dataset.jni.{NativeDataset, NativeDatasetFactory, NativeScanner}
import org.apache.arrow.dataset.scanner.RegularScanOptions
import org.apache.arrow.memory.MemoryUtil
import org.apache.arrow.util.loader.{PartitionedVectorLoaderIterator, VectorLoaderIterator}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.TaskContext
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.conversion.SchemaConversionUtil
import org.arrowspark.arrow.dataset.DatasetFileFormat
import org.arrowspark.arrow.dataset.file.RandomAccessFileDatasetFactory
import org.arrowspark.util.TryUsing

import java.net.URI
import scala.collection.JavaConverters.asScalaIteratorConverter

object ArrowDatasetUtil {
    /**
     * Reads schema as Arrow schema from file collection
     *
     * '''Note''': Setting [[ArrowOptions.mergeSchemas]] in the options determines whether we will read all files and merge schemas, or just read 1 schema.
     * @param file File handle to read from
     * @param options Reading options
     * @return Constructed schema
     */
    def readSchemaSingle(file: FileStatus, options: ArrowOptions): Schema = {
        val fileFormat: DatasetFileFormat = options.fileFormat
        val actualPath = fileStatusToPath(file)
        val dataset = TryUsing.autoCloseTry(new RandomAccessFileDatasetFactory(MemoryUtil.getAllocator(options), MemoryUtil.getMemoryPool(options), fileFormat, actualPath))(factory => factory.finish()).get
        val schema = readSchema(dataset)
        dataset.close()
        schema
    }

    def readSchema(dataset: NativeDataset): Schema = {
        val scanner = dataset.newScan(RegularScanOptions())
        val schema = scanner.schema
        scanner.close()
        schema
    }

    /**
     * Read composed spark schema from multiple file partitions.
     * '''Note''': Currently, reads and returns schema of only first file
     * @param files List of file handles to read schemas from and merge
     * @param options reading and merging options
     * @return Merged Spark schema
     */
    def readSchema(files: Seq[FileStatus], options: ArrowOptions): Option[StructType] = {
        val mergeSchemas = options.mergeSchemas
        if (mergeSchemas) {
            //TODO: Read all files and merge schemas
            Option(SchemaConversionUtil.fromArrowSchema(readSchemaSingle(files.toList.head, options)))
        } else {
            Option(SchemaConversionUtil.fromArrowSchema(readSchemaSingle(files.toList.head, options)))
        }
    }

    def prepareRandomAccessFileRead(path: String, startOffset: Long, length: Long, options: ArrowOptions): NativeDatasetFactory =
        new RandomAccessFileDatasetFactory(MemoryUtil.getAllocator(options), MemoryUtil.getMemoryPool(options), options.interpretedOrDefaultPathFileFormat(path), uriToPath(path), startOffset, length)


    def fileStatusToPath(file: FileStatus): String = file.getPath.toUri.getPath
    def uriToPath(uri: String): String = URI.create(uri).getPath

    /** @return `true` if given format-options are 'splitable' */
    def isSplitable(options: ArrowOptions): Boolean = isSplitable(options.fileFormat)
    def isSplitable(format: DatasetFileFormat): Boolean = {
        format.format match {
            case FileFormat.PARQUET => true
            case FileFormat.CSV => false
            case _ => false
        }
    }

    /**
     * @return List of ScanTask iterators, which contain read data
     * '''Note:''' Result can directly be passed to [[VectorLoaderIterator]] and children
     * '''Warning:''' Do not forget to close dataset and factory after task completion!
     */
    def readFromScanner(scanner: NativeScanner, dataSchema: StructType, dataset: Option[NativeDataset]=None, factory: Option[NativeDatasetFactory]=None): VectorLoaderIterator = {
        val taskList = scanner.scan().iterator().asScala.toList
        val itrList = taskList.map(task => task.scan())

        Option(TaskContext.get()).foreach(value => value.addTaskCompletionListener[Unit](_ => {
            itrList.foreach(_.close())
            taskList.foreach(_.close())
            scanner.close()
            dataset.foreach(d => d.close())
            factory.foreach(f => f.close())
        }))
        new VectorLoaderIterator(itrList.toIterator.flatMap(itr => itr.asScala), dataSchema)
    }

    /**
     * Reads data from a ready scanner, assuming that we are able to attach partition columns
     * @param scanner Scanner to read from
     * @param dataSchema Global data schema of the data we read, provided by user or determined ourselves
     * @param partitionValues Partition values to append
     * @param partitionSchema Schema of the partition column row present in every PartitionedFile. Must be appended to rows
     * @param options Arrow-specific options to use when reading
     * @param dataset Optionally provide the source dataset. If set, we will close it once no longer needed
     * @param factory Optionally provide the source datasetFactory. If set, we will close it once no longer needed
     * @return an Iterator to provide read data
     */
    def readPartitionedFromScanner(scanner: NativeScanner, dataSchema: StructType, partitionValues: InternalRow, partitionSchema: StructType, options: ArrowOptions, dataset: Option[NativeDataset]=None, factory: Option[NativeDatasetFactory]=None): PartitionedVectorLoaderIterator = {
        val taskList = scanner.scan().iterator().asScala.toList
        val itrList = taskList.map(task => task.scan())

        Option(TaskContext.get()).foreach(value => value.addTaskCompletionListener[Unit](_ => {
            itrList.foreach(_.close())
            taskList.foreach(_.close())
            scanner.close()
            dataset.foreach(d => d.close())
            factory.foreach(f => f.close())
        }))

        PartitionedVectorLoaderIterator(itrList.toIterator.flatMap(itr => itr.asScala), partitionValues, partitionSchema, dataSchema, options.memoryMode == MemoryMode.OFF_HEAP)
    }


    /** Return Map with conf settings to be used in ArrowPythonRunner */
    def getPythonRunnerConfMap(conf: SQLConf): Map[String, String] = {
        val timeZoneConf = Seq(SQLConf.SESSION_LOCAL_TIMEZONE.key -> conf.sessionLocalTimeZone)
        val pandasColsByName = Seq(SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key -> conf.pandasGroupedMapAssignColumnsByName.toString)
        val arrowSafeTypeCheck = Seq(SQLConf.PANDAS_ARROW_SAFE_TYPE_CONVERSION.key -> conf.arrowSafeTypeConversion.toString)
        Map(timeZoneConf ++ pandasColsByName ++ arrowSafeTypeCheck: _*)
    }

    def fromAttributes(attributes: Seq[Attribute]): StructType = StructType.fromAttributes(attributes)
}
