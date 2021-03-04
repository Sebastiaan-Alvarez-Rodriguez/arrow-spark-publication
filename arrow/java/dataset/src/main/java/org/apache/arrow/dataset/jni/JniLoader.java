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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The JniLoader for Datasets API's native implementation.
 */
public final class JniLoader {
  private static final JniLoader INSTANCE = new JniLoader();
  private static final List<String> LIBRARY_NAMES = Collections.singletonList("arrow_dataset_jni");

  private AtomicBoolean loaded = new AtomicBoolean(false);

  public static JniLoader get() {
    return INSTANCE;
  }

  private JniLoader() {
  }

  /**
   * If required JNI libraries are not loaded, then load them.
   */
  public void ensureLoaded() {
    if (loaded.compareAndSet(false, true)) {
      LIBRARY_NAMES.forEach(this::load);
    }
  }

  private void load(String name) {
    final String libraryToLoad = System.mapLibraryName(name);
    try {
      File temp = File.createTempFile("jnilib-", ".tmp", new File(System.getProperty("java.io.tmpdir")));
      try (final InputStream is
               = JniWrapper.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
        if (is == null) {
          throw new FileNotFoundException(libraryToLoad);
        }
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        System.load(temp.getAbsolutePath());
      }
    } catch (IOException e) {
      throw new IllegalStateException("error loading native libraries: " + e);
    }
  }
}
