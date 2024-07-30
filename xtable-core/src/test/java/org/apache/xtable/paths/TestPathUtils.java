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
 
package org.apache.xtable.paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestPathUtils {

  @ParameterizedTest
  @MethodSource("inputs")
  void testGetRelativePath(String path, String basePath, String expected) {
    assertEquals(expected, PathUtils.getRelativePath(path, basePath));
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
}
