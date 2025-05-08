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
 
package org.apache.xtable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestHudiInstantUtils {

  @ParameterizedTest
  @MethodSource("instantTimeParseTestCases")
  public void testParseCommitTimeToInstant(String commitTime, Instant expectedInstant) {
    assertEquals(expectedInstant, HudiInstantUtils.parseFromInstantTime(commitTime));
  }

  private static Stream<Arguments> instantTimeParseTestCases() {
    return Stream.of(
        // Original test cases
        Arguments.of("20230120044331843", Instant.parse("2023-01-20T04:43:31.843Z")),
        Arguments.of("20230120044331", Instant.parse("2023-01-20T04:43:31.999Z")),

        // Additional test cases
        Arguments.of("19700101000000000", Instant.parse("1970-01-01T00:00:00.000Z")), // Unix epoch
        Arguments.of(
            "19700101000000",
            Instant.parse("1970-01-01T00:00:00.999Z")), // Unix epoch without millis
        Arguments.of(
            "20251224235959123",
            Instant.parse("2025-12-24T23:59:59.123Z")), // Future date with millis
        Arguments.of(
            "20251224235959",
            Instant.parse("2025-12-24T23:59:59.999Z")), // Future date without millis
        Arguments.of("20200229235959123", Instant.parse("2020-02-29T23:59:59.123Z")), // Leap year
        Arguments.of(
            "20200229235959", Instant.parse("2020-02-29T23:59:59.999Z")) // Leap year without millis
        );
  }

  @ParameterizedTest
  @MethodSource("instantToCommitTestCases")
  public void testInstantToCommit(Instant instant, String expectedCommitTime) {
    assertEquals(expectedCommitTime, HudiInstantUtils.convertInstantToCommit(instant));
  }

  private static Stream<Arguments> instantToCommitTestCases() {
    return Stream.of(
        // Original test cases
        Arguments.of(Instant.parse("2023-01-20T04:43:31.843Z"), "20230120044331843"),
        Arguments.of(Instant.parse("2023-01-20T04:43:31Z"), "20230120044331000"),

        // Additional test cases
        Arguments.of(Instant.parse("1970-01-01T00:00:00Z"), "19700101000000000"), // Unix epoch
        Arguments.of(
            Instant.parse("1970-01-01T00:00:00.123Z"),
            "19700101000000123"), // Unix epoch with millis
        Arguments.of(Instant.parse("2025-12-24T23:59:59Z"), "20251224235959000"), // Future date
        Arguments.of(
            Instant.parse("2025-12-24T23:59:59.999Z"),
            "20251224235959999"), // Future date with max millis
        Arguments.of(Instant.parse("2020-02-29T23:59:59Z"), "20200229235959000"), // Leap year
        Arguments.of(
            Instant.parse("2021-12-31T23:59:59.555Z"), "20211231235959555") // Year end with millis
        );
  }
}
