package org.apache.spark.sql.util.memory

import org.apache.arrow.memory.RootAllocator
import org.apache.spark.TaskContext
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.util.ArrowUtils

object SparkMemoryUtil {
    def rootAllocator: RootAllocator = ArrowUtils.rootAllocator

    def getTaskMemoryManager(tc: TaskContext): TaskMemoryManager = tc.taskMemoryManager
}
