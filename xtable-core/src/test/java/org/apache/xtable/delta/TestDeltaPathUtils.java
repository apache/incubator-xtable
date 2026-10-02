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
 
package org.apache.xtable.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDeltaPathUtils {

  private static Stream<Arguments> fullPathCases() {
    return Stream.of(
        // relative paths are resolved against the table base path
        Arguments.of("s3a://bucket/tab", "part-0.parquet", "s3a://bucket/tab/part-0.parquet"),
        Arguments.of("s3a://bucket/tab/", "part-0.parquet", "s3a://bucket/tab/part-0.parquet"),
        Arguments.of(
            "s3a://bucket/tab",
            "year=2024/part-0.parquet",
            "s3a://bucket/tab/year=2024/part-0.parquet"),
        // absolute paths are used as-is
        Arguments.of(
            "s3a://bucket/tab",
            "s3a://bucket/tab/part-0.parquet",
            "s3a://bucket/tab/part-0.parquet"),
        Arguments.of(
            "s3a://bucket/tab", "s3://bucket/tab/part-0.parquet", "s3://bucket/tab/part-0.parquet"),
        Arguments.of(
            "s3a://bucket/tab",
            "s3://other-bucket/x/part-0.parquet",
            "s3://other-bucket/x/part-0.parquet"),
        Arguments.of(
            "s3a://bucket/tab", "file:///local/t/part-0.parquet", "file:///local/t/part-0.parquet"),
        Arguments.of("/tmp/tab", "/tmp/other/part-0.parquet", "/tmp/other/part-0.parquet"));
  }

  @ParameterizedTest
  @MethodSource("fullPathCases")
  void getFullPathToFile(String tableBasePath, String dataFilePath, String expected) {
    assertEquals(expected, DeltaPathUtils.getFullPathToFile(tableBasePath, dataFilePath));
  }
}
