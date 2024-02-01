/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.filter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

import io.onetable.model.storage.OneDataFile;

class ToRelativePathFilterTest {
  @Test
  void applyToFilesNotInSubTree() {
    ToRelativePathFilter filter = new ToRelativePathFilter();
    String basePath = "scheme:///db/table";
    filter.init(Collections.singletonMap("ToRelativePathFilter.basePath", basePath));

    List<OneDataFile> dataFiles =
        Arrays.asList(
            OneDataFile.builder().physicalPath("abfs://db/table/2019-01/1.parquet").build(),
            OneDataFile.builder().physicalPath("s3://db/other_table/2020-01/2.parquet").build(),
            OneDataFile.builder().physicalPath("scheme://db/other_table/1/2/3/4/3.parquet").build(),
            OneDataFile.builder().physicalPath("file:///db/1.parquet").build());

    List<OneDataFile> expected =
        Arrays.asList(
            OneDataFile.builder().physicalPath("abfs://db/table/2019-01/1.parquet").build(),
            OneDataFile.builder().physicalPath("s3://db/other_table/2020-01/2.parquet").build(),
            OneDataFile.builder().physicalPath("scheme://db/other_table/1/2/3/4/3.parquet").build(),
            OneDataFile.builder().physicalPath("file:///db/1.parquet").build());

    List<OneDataFile> result = filter.apply(dataFiles);
    assertNotNull(result);
    assertEquals(dataFiles.size(), result.size());
    assertEquals(expected, result);
  }

  @Test
  void applyToFilesInSubTree() {
    ToRelativePathFilter filter = new ToRelativePathFilter();
    String basePath = "scheme://db/table";
    filter.init(Collections.singletonMap("ToRelativePathFilter.basePath", basePath));

    List<OneDataFile> dataFiles =
        Arrays.asList(
            OneDataFile.builder().physicalPath(basePath + "/2019-01/1.parquet").build(),
            OneDataFile.builder().physicalPath(basePath + "/2020-01/2.parquet").build(),
            OneDataFile.builder().physicalPath(basePath + "/1/2/3/4/3.parquet").build(),
            OneDataFile.builder().physicalPath("1.parquet").build());

    List<OneDataFile> expected =
        Arrays.asList(
            OneDataFile.builder().physicalPath("2019-01/1.parquet").build(),
            OneDataFile.builder().physicalPath("2020-01/2.parquet").build(),
            OneDataFile.builder().physicalPath("1/2/3/4/3.parquet").build(),
            OneDataFile.builder().physicalPath("1.parquet").build());

    assertEquals(expected, filter.apply(dataFiles));
  }

  private static Stream<Arguments> inputs() {
    return Stream.of(
        Arguments.of("/relative/path", "file:///absolute/path", "relative/path"),
        Arguments.of(
            "file:///absolute/path/to/file.parquet", "file:///absolute/path", "to/file.parquet"),
        Arguments.of(
            "file:///absolute/path/to/file.parquet", "file:/absolute/path", "to/file.parquet"),
        Arguments.of(
            "file:/absolute/path/to/file.parquet", "file:///absolute/path", "to/file.parquet"),
        Arguments.of("s3://absolute/path/to/file.parquet", "s3://absolute/path", "to/file.parquet"),
        Arguments.of(
            "s3a://absolute/path/to/file.parquet", "s3a://absolute/path", "to/file.parquet"),
        Arguments.of(
            "s3a://absolute/path/to/file.parquet", "s3://absolute/path", "to/file.parquet"),
        Arguments.of(
            "s3://absolute/path/to/file.parquet", "s3a://absolute/path", "to/file.parquet"));
  }

  @Test
  void getIdentifier() {
    assertEquals("ToRelativePathFilter", new ToRelativePathFilter().getIdentifier());
  }
}
