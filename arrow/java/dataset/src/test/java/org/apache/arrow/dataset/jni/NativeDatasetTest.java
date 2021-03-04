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

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.arrow.dataset.DatasetTypes;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystem;
import org.apache.arrow.dataset.file.SingleFileDatasetFactory;
import org.apache.arrow.dataset.filter.Filter;
import org.apache.arrow.dataset.filter.FilterImpl;
import org.apache.arrow.dataset.fragment.DataFragment;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.ReservationListener;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class NativeDatasetTest {

  private String sampleParquet() {
    return NativeDatasetTest.class.getResource(File.separator + "userdata1.parquet").getPath();
  }

  private void testDatasetFactoryEndToEnd(DatasetFactory factory, int taskCount, int vectorCount, int rowCount) {
    Schema schema = factory.inspect();

    Assert.assertEquals("Schema<registration_dttm: Timestamp(NANOSECOND, null), id: Int(32, true), first_name: " +
            "Utf8[dictionary: 0], last_name: Utf8[dictionary: 1], email: Utf8[dictionary: 2], " +
            "gender: Utf8[dictionary: 3], ip_address: Utf8[dictionary: 4], cc: Utf8[dictionary: 5]," +
            " country: Utf8[dictionary: 6], birthdate: Utf8[dictionary: 7], salary: FloatingPoint(DOUBLE), " +
            "title: Utf8[dictionary: 8], comments: Utf8[dictionary: 9]>(metadata: {org.apache.spark.version=3.0.0, " +
            "org.apache.spark.sql.parquet.row.metadata={\"type\":\"struct\",\"fields\":[{\"name\":" +
            "\"registration_dttm\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":" +
            "\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"first_name\",\"type" +
            "\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"last_name\",\"type\":\"string\"," +
            "\"nullable\":true,\"metadata\":{}},{\"name\":\"email\",\"type\":\"string\",\"nullable\":true," +
            "\"metadata\":{}},{\"name\":\"gender\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
            "{\"name\":\"ip_address\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cc\"," +
            "\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"country\",\"type\":\"string\"," +
            "\"nullable\":true,\"metadata\":{}},{\"name\":\"birthdate\",\"type\":\"string\",\"nullable\":true," +
            "\"metadata\":{}},{\"name\":\"salary\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}," +
            "{\"name\":\"title\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"comments\"," +
            "\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}})",
        schema.toString());

    Dataset dataset = factory.finish();
    Assert.assertNotNull(dataset);


    List<? extends ScanTask> scanTasks = collect(dataset
        .newScan(new ScanOptions(new String[0], Filter.EMPTY, 100)).scan());
    Assert.assertEquals(taskCount, scanTasks.size());

    List<? extends ScanTask.ArrowBundledVectors> data = scanTasks.stream()
        .flatMap(t -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(t.scan(), Spliterator.ORDERED), false))
        .collect(Collectors.toList());
    Assert.assertNotNull(data);
    // 1000 rows total in file userdata1.parquet
    Assert.assertEquals(vectorCount, data.size());
    Assert.assertEquals(rowCount, data.stream()
        .mapToInt(a -> a.valueVectors.getRowCount()).sum());


    // FIXME when using list field:
    // FIXME it seems c++ parquet reader doesn't create buffers for list field it self,
    // FIXME as a result Java side buffer pointer gets out of bound.
  }

  @Test
  public void testLocalFs() {
    String path = sampleParquet();
    DatasetFactory discovery = new SingleFileDatasetFactory(
        new RootAllocator(Long.MAX_VALUE), NativeMemoryPool.getDefault(), FileFormat.PARQUET, FileSystem.LOCAL,
        path);
    testDatasetFactoryEndToEnd(discovery, 1, 10, 1000);
  }

  @Test
  public void testSplitFile() {
    String path = sampleParquet();
    DatasetFactory discovery = new SingleFileDatasetFactory(
        new RootAllocator(Long.MAX_VALUE), NativeMemoryPool.getDefault(), FileFormat.PARQUET, FileSystem.LOCAL,
        path, 0L, 0L);
    testDatasetFactoryEndToEnd(discovery, 0, 0, 0);
    discovery = new SingleFileDatasetFactory(
        new RootAllocator(Long.MAX_VALUE), NativeMemoryPool.getDefault(), FileFormat.PARQUET, FileSystem.LOCAL,
        path, 0L, 100000L);
    testDatasetFactoryEndToEnd(discovery, 1, 10, 1000);
    discovery = new SingleFileDatasetFactory(
        new RootAllocator(Long.MAX_VALUE), NativeMemoryPool.getDefault(), FileFormat.PARQUET, FileSystem.LOCAL,
        path, 100000L, 200000L);
    testDatasetFactoryEndToEnd(discovery, 0, 0, 0);
  }

  @Ignore
  public void testHdfsWithFileProtocol() {
    String path = "file:" + sampleParquet();
    DatasetFactory discovery = new SingleFileDatasetFactory(
        new RootAllocator(Long.MAX_VALUE), NativeMemoryPool.getDefault(), FileFormat.PARQUET, FileSystem.HDFS,
        path);
    testDatasetFactoryEndToEnd(discovery, 1, 10, 1000);
  }

  @Ignore
  public void testHdfsWithHdfsProtocol() {
    // If using libhdfs rather than libhdfs3:
    // Set JAVA_HOME and HADOOP_HOME first. See hdfs_internal.cc:128
    // And libhdfs requires having hadoop java libraries set within CLASSPATH. See 1. https://hadoop.apache.org/docs/r2.7.7/hadoop-project-dist/hadoop-hdfs/LibHdfs.html, 2. https://arrow.apache.org/docs/python/filesystems.html#hadoop-file-system-hdfs

    // If using libhdfs3, make sure ARROW_LIBHDFS3_DIR is set.
    // Install libhdfs3: https://medium.com/@arush.xtremelife/connecting-hadoop-hdfs-with-python-267234bb68a2
    String path = "hdfs://localhost:9000/userdata1.parquet?use_hdfs3=1";
    DatasetFactory discovery = new SingleFileDatasetFactory(
        new RootAllocator(Long.MAX_VALUE), NativeMemoryPool.getDefault(), FileFormat.PARQUET, FileSystem.HDFS,
        path);
    testDatasetFactoryEndToEnd(discovery, 1, 10, 1000);
  }

  @Test
  public void testScanner() throws Exception {
    String path = sampleParquet();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    NativeDatasetFactory factory = new SingleFileDatasetFactory(
        allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, FileSystem.LOCAL,
        path);
    Schema schema = factory.inspect();
    NativeDataset dataset = factory.finish(schema);
    ScanOptions scanOptions = new ScanOptions(new String[]{"id", "title"}, Filter.EMPTY, 100);
    Scanner scanner = dataset.newScan(scanOptions);
    List<? extends ScanTask> scanTasks = collect(scanner.scan());
    Assert.assertEquals(1, scanTasks.size());

    ScanTask scanTask = scanTasks.get(0);
    ScanTask.Itr itr = scanTask.scan();
    int vsrCount = 0;
    VectorSchemaRoot vsr = null;
    while (itr.hasNext()) {
      // FIXME VectorSchemaRoot is not actually something ITERABLE.// Using a reader convention instead.
      vsrCount++;
      vsr = itr.next().valueVectors;
      Assert.assertEquals(100, vsr.getRowCount());

      // check if projector is applied
      Assert.assertEquals(2,
          vsr.getSchema().getFields().size());
    }
    Assert.assertEquals(10, vsrCount);

    if (vsr != null) {
      vsr.close();
    }
    itr.close();
    allocator.close();
  }

  @Test
  public void testScannerWithFilter() throws Exception {
    String path = sampleParquet();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    NativeDatasetFactory factory = new SingleFileDatasetFactory(
        allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, FileSystem.LOCAL,
        path);
    Schema schema = factory.inspect();
    NativeDataset dataset = factory.finish(schema);
    // condition id = 500
    DatasetTypes.Condition condition = DatasetTypes.Condition.newBuilder()
        .setRoot(DatasetTypes.TreeNode.newBuilder()
            .setCpNode(DatasetTypes.ComparisonNode.newBuilder()
                .setOpName("equal") // todo make op names enumerable
                .setLeftArg(
                    DatasetTypes.TreeNode.newBuilder().setFieldNode(
                        DatasetTypes.FieldNode.newBuilder().setName("id").build()).build())
                .setRightArg(
                    DatasetTypes.TreeNode.newBuilder().setIntNode(
                        DatasetTypes.IntNode.newBuilder().setValue(500).build()).build())
                .build())
            .build())
        .build();
    Filter filter = new FilterImpl(condition);
    ScanOptions scanOptions = new ScanOptions(new String[]{"id", "title"}, filter, 100);
    Scanner scanner = dataset.newScan(scanOptions);
    List<? extends ScanTask> scanTasks = collect(scanner.scan());
    Assert.assertEquals(1, scanTasks.size());

    ScanTask scanTask = scanTasks.get(0);
    ScanTask.Itr itr = scanTask.scan();
    VectorSchemaRoot vsr = null;
    int rowCount = 0;
    while (itr.hasNext()) {
      // FIXME VectorSchemaRoot is not actually something ITERABLE. Using a reader convention instead.
      ScanTask.ArrowBundledVectors bundledVectors = itr.next();
      vsr = bundledVectors.valueVectors;
      Map<Long, Dictionary> dvs = bundledVectors.dictionaryVectors;
      // only the line with id = 500 selected
      rowCount += vsr.getRowCount();

      // check if projector is applied
      Assert.assertEquals(2,
          vsr.getSchema().getFields().size());

      // dictionaries
      Assert.assertEquals(1, dvs.size());
    }
    Assert.assertEquals(1, rowCount);

    if (vsr != null) {
      vsr.close();
    }
    itr.close();
    allocator.close();
  }

  @Test
  public void testScannerWithCustomMemoryReservation() throws Exception {
    final AtomicLong reserved = new AtomicLong(0L);
    final NativeMemoryPool pool = NativeMemoryPool.createListenable(new ReservationListener() {
      @Override
      public void reserve(long size) {
        reserved.getAndAdd(size);
      }

      @Override
      public void unreserve(long size) {
        reserved.getAndAdd(-size);
      }
    });
    long resBefore = reserved.get();
    String path = sampleParquet();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    NativeDatasetFactory factory = new SingleFileDatasetFactory(
        allocator, pool, FileFormat.PARQUET, FileSystem.LOCAL,
        path);
    Schema schema = factory.inspect();
    NativeDataset dataset = factory.finish(schema);
    ScanOptions scanOptions = new ScanOptions(new String[]{"id", "title"}, Filter.EMPTY, 100);
    Scanner scanner = dataset.newScan(scanOptions);
    List<? extends ScanTask> scanTasks = collect(scanner.scan());
    Assert.assertEquals(1, scanTasks.size());

    ScanTask scanTask = scanTasks.get(0);
    ScanTask.Itr itr = scanTask.scan();
    int vsrCount = 0;
    VectorSchemaRoot vsr = null;
    while (itr.hasNext()) {
      // FIXME VectorSchemaRoot is not actually something ITERABLE.// Using a reader convention instead.
      vsrCount++;
      vsr = itr.next().valueVectors;
      Assert.assertEquals(100, vsr.getRowCount());

      // check if projector is applied
      Assert.assertEquals(2,
          vsr.getSchema().getFields().size());
    }
    Assert.assertEquals(10, vsrCount);

    long res = reserved.get();

    if (vsr != null) {
      vsr.close();
    }
    itr.close();
    allocator.close();

    long resAfter = reserved.get();
    Assert.assertNotEquals(res, resBefore);
    Assert.assertEquals(res - resBefore, res - resAfter);
  }

  // TODO fix for empty projector. Currently empty projector is treated as projection on all available columns.
  @Ignore
  public void testScannerWithEmptyProjector() {
    String path = sampleParquet();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    NativeDatasetFactory factory = new SingleFileDatasetFactory(
        allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, FileSystem.LOCAL,
        path);
    Schema schema = factory.inspect();
    NativeDataset dataset = factory.finish(schema);
    ScanOptions scanOptions = new ScanOptions(new String[]{}, Filter.EMPTY, 100);
    Scanner scanner = dataset.newScan(scanOptions);
    List<? extends ScanTask> scanTasks = collect(scanner.scan());
    Assert.assertEquals(1, scanTasks.size());

    ScanTask scanTask = scanTasks.get(0);
    ScanTask.Itr itr = scanTask.scan();
    VectorSchemaRoot vsr = null;
    int rowCount = 0;
    while (itr.hasNext()) {
      // FIXME VectorSchemaRoot is not actually something ITERABLE. Using a reader convention instead.
      vsr = itr.next().valueVectors;
      rowCount += vsr.getRowCount();

      // check if projector is applied
      Assert.assertEquals("Schema<>",
          vsr.getSchema().toString());
    }
    Assert.assertEquals(1, rowCount);

    if (vsr != null) {
      vsr.close();
    }
    allocator.close();
  }

  @Ignore
  public void testFilter() {
    // todo
  }

  @Ignore
  public void testProjector() {
    // todo
  }

  private <T> List<T> collect(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
  }

  private <T> List<T> collect(Iterator<T> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
        .collect(Collectors.toList());
  }
}
