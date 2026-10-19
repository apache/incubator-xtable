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
        // Old version of metadata and JSON
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
            "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"version\":0}"),
        // New version of metadata and JSON with `sourceTableFormat` and `sourceIdentifier` fields
        Arguments.of(
            TableSyncMetadata.of(
                Instant.parse("2020-07-04T10:15:30.00Z"),
                Arrays.asList(
                    Instant.parse("2020-08-21T11:15:30.00Z"),
                    Instant.parse("2024-01-21T12:15:30.00Z")),
                "TEST",
                "0"),
            "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"instantsToConsiderForNextSync\":[\"2020-08-21T11:15:30Z\",\"2024-01-21T12:15:30Z\"],\"version\":0,\"sourceTableFormat\":\"TEST\",\"sourceIdentifier\":\"0\"}"),
        Arguments.of(
            TableSyncMetadata.of(
                Instant.parse("2020-07-04T10:15:30.00Z"), Collections.emptyList(), "TEST", "0"),
            "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"instantsToConsiderForNextSync\":[],\"version\":0,\"sourceTableFormat\":\"TEST\",\"sourceIdentifier\":\"0\"}"),
        Arguments.of(
            TableSyncMetadata.of(Instant.parse("2020-07-04T10:15:30.00Z"), null, "TEST", "0"),
            "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"version\":0,\"sourceTableFormat\":\"TEST\",\"sourceIdentifier\":\"0\"}"),
        // Version 0 with the `latestTableOperationIdentifier` field a pluggable table format writes
        Arguments.of(
            TableSyncMetadata.of(
                Instant.parse("2020-07-04T10:15:30.00Z"),
                Collections.emptyList(),
                "HUDI",
                "20200704101530000",
                "20200704101530000.commit"),
            "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"instantsToConsiderForNextSync\":[],\"version\":0,\"sourceTableFormat\":\"HUDI\",\"sourceIdentifier\":\"20200704101530000\",\"latestTableOperationIdentifier\":\"20200704101530000.commit\"}"));
  }

  @Test
  void ignoresFieldsAddedByANewerWriter() {
    // The blob lives in target-table metadata, so a reader on an older version has to keep
    // parsing a version-0 payload that a newer writer extended with fields it does not know.
    TableSyncMetadata parsed =
        TableSyncMetadata.fromJson(
                "{\"lastInstantSynced\":\"2020-07-04T10:15:30Z\",\"instantsToConsiderForNextSync\":[],\"version\":0,\"fieldFromANewerWriter\":\"ignored\"}")
            .get();
    assertEquals(
        TableSyncMetadata.of(Instant.parse("2020-07-04T10:15:30.00Z"), Collections.emptyList()),
        parsed);
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
