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
 
package org.apache.xtable.model.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.xtable.model.exception.ParseException;

class TestTableSyncMetadata {

  @ParameterizedTest
  @MethodSource("provideMetadataAndJson")
  void jsonRoundTrip(TableSyncMetadata metadata, String expectedJson) {
    assertEquals(expectedJson, metadata.toJson());
    assertEquals(metadata, TableSyncMetadata.fromJson(expectedJson).get());
  }

  private static Stream<Arguments> provideMetadataAndJson() {
    return Stream.of(
        Arguments.of(
            TableSyncMetadata.of(
                Instant.parse("2020-07-04T10:15:30.00Z"),
                Arrays.asList(
                    Instant.parse("2020-08-21T11:15:30.00Z"),
                    Instant.parse("2024-01-21T12:15:30.00Z"))),
            "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"instantsToConsiderForNextSync\":[\"2020-08-21T11:15:30Z\",\"2024-01-21T12:15:30Z\"],\"version\":0}"),
        Arguments.of(
            TableSyncMetadata.of(Instant.parse("2020-07-04T10:15:30.00Z"), Collections.emptyList()),
            "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"instantsToConsiderForNextSync\":[],\"version\":0}"),
        Arguments.of(
            TableSyncMetadata.of(Instant.parse("2020-07-04T10:15:30.00Z"), null),
            "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"version\":0}"));
  }

  @Test
  void failToParseJsonFromNewerVersion() {
    assertThrows(
        ParseException.class,
        () ->
            TableSyncMetadata.fromJson(
                "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"instantsToConsiderForNextSync\":[\"2020-08-21T11:15:30Z\",\"2024-01-21T12:15:30Z\"],\"version\":1}"));
  }

  @Test
  void failToParseJsonWithMissingLastSyncedInstant() {
    assertThrows(
        ParseException.class,
        () ->
            TableSyncMetadata.fromJson(
                "{\"instantsToConsiderForNextSync\":[\"2020-08-21T11:15:30Z\",\"2024-01-21T12:15:30Z\"],\"version\":0}"));
  }
}
