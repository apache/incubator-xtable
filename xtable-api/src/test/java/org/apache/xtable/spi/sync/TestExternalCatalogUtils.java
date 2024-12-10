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
 
package org.apache.xtable.spi.sync;

import static org.apache.xtable.spi.sync.CatalogUtils.hasStorageDescriptorLocationChanged;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.xtable.model.exception.CatalogRefreshException;

public class TestExternalCatalogUtils {

  static Stream<Arguments> storageLocationTestArgs() {
    return Stream.of(
        Arguments.of("s3://bucket/table/v1", "s3://bucket/table/v2", true),
        Arguments.of("s3://bucket/table1/v1", "s3://bucket/table2/v1", true),
        Arguments.of("file:///var/lib/bucket/table/v1", "file:///var/lib/bucket/table/v2/", true),
        Arguments.of("s3://bucket/table/v1", "s3://bucket/table/v1", false),
        Arguments.of("s3a://bucket/table/v1", "s3://bucket/table/v1/", false),
        Arguments.of("s3://bucket/table/v1", "s3a://bucket/table/v1", false),
        Arguments.of("s3://bucket/table/v1/", "s3a://bucket/table/v1", false),
        Arguments.of("/var/lib/bucket/table/v1", "/var/lib/bucket/table/v1/", false),
        Arguments.of("file:///var/lib/bucket/table/v1", "file:///var/lib/bucket/table/v1/", false));
  }

  static Stream<Arguments> storageLocationTestArgsException() {
    return Stream.of(
        Arguments.of(
            "s3://bucket/table/v1",
            "gs://bucket/table/v1",
            new CatalogRefreshException(
                "Storage scheme has changed for table catalogStorageDescriptorUri s3://bucket/table/v1 basePathUri gs://bucket/table/v1")));
  }

  @ParameterizedTest
  @MethodSource("storageLocationTestArgs")
  void testHasStorageLocationChanged(String storageLocation, String basePath, boolean expected) {
    assertEquals(expected, hasStorageDescriptorLocationChanged(storageLocation, basePath));
  }

  @ParameterizedTest
  @MethodSource("storageLocationTestArgsException")
  void testHasStorageLocationChangedException(
      String storageLocation, String basePath, Exception exception) {
    assertThrows(
        exception.getClass(),
        () -> hasStorageDescriptorLocationChanged(storageLocation, basePath),
        exception.getMessage());
  }
}
