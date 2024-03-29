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

package org.apache.arrow.memory;

import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

/**
 * Context for relevant classes of NativeDataSource.
 */
public class NativeContext {
  private final BufferAllocator allocator;
  private final NativeMemoryPool memoryPool;

  /**
   * Constructor.
   *
   * @param allocator The allocator in use.
   * @param memoryPool Native memory pool.
   */
  public NativeContext(BufferAllocator allocator, NativeMemoryPool memoryPool) {
    this.memoryPool = memoryPool;
    Preconditions.checkArgument(allocator instanceof BaseAllocator,
        "currently only instance of BaseAllocator supported");
    this.allocator = allocator;
  }

  /**
   * Returns the allocator which is in use.
   */
  public BaseAllocator getAllocator() {
    return (BaseAllocator) allocator;
  }

  /**
   * Returns the native memory pool.
   */
  public NativeMemoryPool getMemoryPool() {
    return memoryPool;
  }
}
