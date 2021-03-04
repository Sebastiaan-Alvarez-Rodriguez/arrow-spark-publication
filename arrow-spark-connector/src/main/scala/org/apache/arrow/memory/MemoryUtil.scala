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

package org.apache.arrow.memory

import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.spark.TaskContext
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.util.ArrowOptions
import org.apache.spark.sql.util.memory.SparkMemoryUtil
import org.apache.spark.util.TaskCompletionListener

import java.util.UUID
import javax.annotation.Nonnull
import scala.collection.JavaConverters.asScalaBufferConverter

object MemoryUtil {

    /** Small extension on [[MemoryConsumer]] */
    protected class NativeSQLMemoryConsumer(taskMemoryManager: TaskMemoryManager, offHeapMemory: Boolean) extends MemoryConsumer(taskMemoryManager, taskMemoryManager.pageSizeBytes, if (offHeapMemory) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP) {

        override def spill(size: Long, trigger: MemoryConsumer): Long = 0L

        override def acquireMemory(size: Long): Long = {
            if (size == 0) return 0L
            val granted: Long = super.acquireMemory(size)
            if (granted < size) throw new OutOfMemoryException(s"Not enough spark off-heap execution memory. Acquired: $size, granted: $granted. Try tweaking config option spark.memory.offHeap.size to get larger space to run this application.")
            granted
        }
    }

    /** Quick listener to forward allocation/deallocation calls to a Spark [[MemoryConsumer]] */
    protected class SparkAllocationListener(val consumer: MemoryConsumer) extends AllocationListener {
        override def onPreAllocation(size: Long): Unit = consumer.acquireMemory(size)

        override def onRelease(size: Long): Unit = consumer.freeMemory(size)
    }

    /** Quick listener to forward reservation calls to a Spark [[MemoryConsumer]] */
    protected class SparkReservationListener(val consumer: MemoryConsumer) extends ReservationListener {
        override def reserve(size: Long): Unit = consumer.acquireMemory(size)

        override def unreserve(size: Long): Unit = consumer.freeMemory(size)
    }

    /** Class holding base resources for a given [[TaskMemoryManager]] (found in a valid [[TaskContext]]) */
    protected class TaskMemoryResources(taskMemoryManager: TaskMemoryManager, offHeapMemory: Boolean) {
        if (TaskContext.get() == null) throw new IllegalStateException("Creating TaskMemoryResources instance out of Spark task")

        def this(tc: TaskContext, offHeapMemory: Boolean) = this(SparkMemoryUtil.getTaskMemoryManager(tc), offHeapMemory)

        val defaultAllocator: BaseAllocator = {
            val al = new SparkAllocationListener(new NativeSQLMemoryConsumer(taskMemoryManager, offHeapMemory))
            val parent = SparkMemoryUtil.rootAllocator
            parent.newChildAllocator(s"Spark Managed Allocator - ${UUID.randomUUID().toString}", al, 0, parent.getLimit).asInstanceOf[BaseAllocator]
        }

        val defaultMemoryPool: NativeMemoryPool = {
            val rl = new SparkReservationListener(new NativeSQLMemoryConsumer(taskMemoryManager, offHeapMemory))
            NativeMemoryPool.createListenable(rl)
        }

        private val allocators = new java.util.ArrayList[BufferAllocator]()
        allocators.add(defaultAllocator)

        private val memoryPools = new java.util.ArrayList[NativeMemoryPool]()
        memoryPools.add(defaultMemoryPool)

        def createSpillableMemoryPool(): NativeMemoryPool = {
            val rl = new SparkReservationListener(new NativeSQLMemoryConsumer(taskMemoryManager, offHeapMemory))
            val pool = NativeMemoryPool.createListenable(rl)
            memoryPools.add(pool)
            pool
        }

        def createSpillableAllocator(): BaseAllocator = {
            val al = new SparkAllocationListener(new NativeSQLMemoryConsumer(taskMemoryManager, offHeapMemory))
            val parent = SparkMemoryUtil.rootAllocator
            val alloc = parent.newChildAllocator(s"Spark Managed Allocator - ${UUID.randomUUID().toString}", al, 0, parent.getLimit).asInstanceOf[BaseAllocator]
            allocators.add(alloc)
            alloc
        }

        private def close(allocator: BufferAllocator): Unit = {
            allocator.getChildAllocators.forEach(close(_))
            allocator.close()
        }

        /**
         * Close the allocator quietly without having any OOM errors thrown. We rely on Spark's memory
         * management system to detect possible memory leaks after the task get successfully down. Any
         * leak shown right here is possibly not actual because buffers may be cleaned up after
         * this check code is executed. Having said that developers should manage to make sure
         * the specific clean up logic of operators is registered at last of the program which means
         * it will be executed earlier.
         *
         * @see org.apache.spark.executor.Executor.TaskRunner#run()
         */
        private def softClose(allocator: BufferAllocator): Unit = leakedAllocators.add(allocator)

        private def close(pool: NativeMemoryPool): Unit = pool.close()

        private def softClose(pool: NativeMemoryPool): Unit = leakedMemoryPools.add(pool)

        def release(): Unit = {
            for (allocator <- allocators.asScala) {
                val allocated = allocator.getAllocatedMemory
                if (allocated == 0L)
                    close(allocator)
                else
                    softClose(allocator)
            }

            for (pool <- memoryPools.asScala) {
                val allocated = pool.getBytesAllocated
                if (allocated == 0L)
                    close(pool)
                else
                    softClose(pool)
            }
        }
    }


    protected val taskToResourcesMap = new java.util.IdentityHashMap[TaskContext, TaskMemoryResources]()
    protected val leakedAllocators = new java.util.Vector[BufferAllocator]()
    protected val leakedMemoryPools = new java.util.Vector[NativeMemoryPool]()

    def getTaskMemoryResources(@Nonnull tc: TaskContext, offheapMemory: Boolean): TaskMemoryResources = {
        taskToResourcesMap.synchronized {
            if (!taskToResourcesMap.containsKey(tc)) {
                val resources = new TaskMemoryResources(tc, offheapMemory)
                TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
                    override def onTaskCompletion(context: TaskContext): Unit = taskToResourcesMap.synchronized(taskToResourcesMap.remove(context).release())
                })
                taskToResourcesMap.put(tc, resources)
                return resources
            }
            taskToResourcesMap.get(tc)
        }
    }

    /** @return default (on-heap) memory pool */
    def getMemoryPool: NativeMemoryPool = getMemoryPool(false)
    /** @return Memory pool with on-heap/off-heap memory, depending on settings */
    def getMemoryPool(options: ArrowOptions): NativeMemoryPool = getMemoryPool(options.memoryMode == MemoryMode.OFF_HEAP)

    def getMemoryPool(offHeapMemeory: Boolean): NativeMemoryPool = {
        val tc = Option(TaskContext.get())
        tc match {
            case Some(value) => getTaskMemoryResources(value, offHeapMemeory).defaultMemoryPool
            case None => NativeMemoryPool.getDefault
        }
    }

    /** @return default (on-heap) allocator */
    def getAllocator: BaseAllocator = getAllocator(false)
    /** @return on-heap/off-heap allocator, depending on settings */
    def getAllocator(options: ArrowOptions): BaseAllocator = getAllocator(options.memoryMode == MemoryMode.OFF_HEAP)

    def getAllocator(offHeapMemeory: Boolean): BaseAllocator = {
        val tc = Option(TaskContext.get())
        tc match {
            case Some(value) => getTaskMemoryResources(value, offHeapMemeory).defaultAllocator
            case None => SparkMemoryUtil.rootAllocator
        }
    }

}
