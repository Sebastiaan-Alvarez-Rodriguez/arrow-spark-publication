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

import org.apache.arrow.dataset.fragment.DataFragment;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.memory.NativeContext;

/**
 * Native implementation of {@link Dataset}.
 */
public class NativeDataset implements Dataset, AutoCloseable {

  private final NativeContext context;
  private final long datasetId;

  public NativeDataset(NativeContext context, long datasetId) {
    this.context = context;
    this.datasetId = datasetId;
  }

  @Override
  public Iterable<? extends DataFragment> getFragments(ScanOptions options) {
    throw new UnsupportedOperationException("Use of getFragments() on NativeDataset is currently forbidden. " +
        "Try creating scanners instead");
  }

  @Override
  public NativeScanner newScan(ScanOptions options) {
    long scannerId = JniWrapper.get().createScanner(datasetId, options.getColumns(),
        options.getFilter().toByteArray(), options.getBatchSize(), context.getMemoryPool().getNativeInstanceId());
    return new NativeScanner(context, scannerId);
  }

  public long getDatasetId() {
    return datasetId;
  }

  @Override
  public void close() {
    JniWrapper.get().closeDataset(datasetId);
  }
}
