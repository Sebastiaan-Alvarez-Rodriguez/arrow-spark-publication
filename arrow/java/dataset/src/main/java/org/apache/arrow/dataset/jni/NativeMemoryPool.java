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

package org.apache.arrow.dataset.jni;


import org.apache.arrow.memory.ReservationListener;

/**
 * Native memory pool's Java mapped instance.
 */
public class NativeMemoryPool implements AutoCloseable {
  private final long nativeInstanceId;

  static {
    JniLoader.get().ensureLoaded();
  }

  public NativeMemoryPool(long nativeInstanceId) {
    this.nativeInstanceId = nativeInstanceId;
  }

  public static NativeMemoryPool getDefault() {
    return new NativeMemoryPool(getDefaultMemoryPool());
  }

  public static NativeMemoryPool createListenable(ReservationListener listener) {
    return new NativeMemoryPool(createListenableMemoryPool(listener));
  }

  public long getBytesAllocated() {
    return bytesAllocated(nativeInstanceId);
  }

  public long getNativeInstanceId() {
    return nativeInstanceId;
  }

  @Override
  public void close() throws Exception {
    releaseMemoryPool(nativeInstanceId);
  }

  private static native long getDefaultMemoryPool();

  private static native long createListenableMemoryPool(ReservationListener listener);

  private static native void releaseMemoryPool(long id);

  private static native long bytesAllocated(long id);
}
