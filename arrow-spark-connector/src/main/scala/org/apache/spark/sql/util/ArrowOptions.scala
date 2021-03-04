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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.arrowspark.arrow.dataset.DatasetFileFormat

import scala.util.Try

/** ArrowOptions Companion object */
object ArrowOptions extends Logging {
    final val mergeSchemasProperty: String = "arrow.read.mergeSchema".toLowerCase
    final val fileFormatProperty: String = "arrow.read.fileFormat".toLowerCase
    final val offheapStorageProperty: String = "arrow.read.offHeapStorage".toLowerCase

    private val DefaultMergeSchemas: String = true.toString
    private val DefaultFileFormat: String = "pq"
    private val DefaultOffHeapStorage: Boolean = false


    def apply(sparkConf: SparkConf, options: Map[String, String]): ArrowOptions = apply(sparkConf, options, None)

    def apply(sparkConf: SparkConf, options: Map[String, String], path: String): ArrowOptions = apply(sparkConf, options, Option(path))
    def apply(sparkConf: SparkConf, options: Map[String, String], path: Path): ArrowOptions = apply(sparkConf, options, Option(path.toString))
    def apply(sparkConf: SparkConf, options: Map[String, String], path: Option[String]): ArrowOptions = {
        val mergeSchemas: Boolean = options.getOrElse(ArrowOptions.mergeSchemasProperty, ArrowOptions.DefaultMergeSchemas).toBoolean

        val fileFormat: DatasetFileFormat = options.get(fileFormatProperty) match {
            case Some(value) => DatasetFileFormat.fromString(value)
            case None => path match {
                case Some(value) => Try(DatasetFileFormat.fromPathLike(value)).getOrElse({
                    logWarning("Could not determine fileformat from path. Use files ending on .pq, .csv to make automatic detection work.\nAlternatively, use session.read.option(ArrowOptions.fileFormatProperty, \"pq\").arrow(path) to manually set format for all files.\n")
                    DatasetFileFormat.fromString(DefaultFileFormat)
                })
                case None => DatasetFileFormat.fromString(DefaultFileFormat)
            }
        }
        val memoryMode: MemoryMode = if (sparkConf.getBoolean("spark.memory.offHeap.enabled", defaultValue=DefaultOffHeapStorage)) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP
        val offheapSize: Long = sparkConf.getLong("spark.memory.offHeap.size", 0)
        new ArrowOptions(mergeSchemas, fileFormat, memoryMode, offheapSize)
    }
}

/**
 * Simple class containing several options, which determine how Arrow operates.
 * @param mergeSchemas If `true`, merges schemas of all specified files. If false, uses only the first-returned file to infer the schema from.
 * @param fileFormat Type of data we read
 * @param memoryMode Either [[MemoryMode.OFF_HEAP]] or [[MemoryMode.ON_HEAP]]. Determines whether Arrow will use on-heap or (slightly faster) off-heap intermediate buffers
 * @param offheapSize The size (in bytes) to allocate for off-heap memory. A very tricky number. Seems to be constrained to pow(2,31)-1.
 */
class ArrowOptions(
    val mergeSchemas: Boolean,
    val fileFormat: DatasetFileFormat,
    val memoryMode: MemoryMode,
    val offheapSize: Long) extends Serializable {

    def interpretedOrDefaultPathFileFormat(path: String): DatasetFileFormat = Try(DatasetFileFormat.fromPathLike(path)).getOrElse(fileFormat)
}

