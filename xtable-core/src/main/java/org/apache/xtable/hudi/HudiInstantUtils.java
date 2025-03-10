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

import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH;
import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.SECS_INSTANT_ID_LENGTH;
import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.SECS_INSTANT_TIMESTAMP_FORMAT;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;

import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;

import org.apache.xtable.model.exception.ParseException;

class HudiInstantUtils {
  private static final ZoneId ZONE_ID = ZoneId.of("UTC");

  // Unfortunately millisecond format is not parsable as is
  // https://bugs.openjdk.java.net/browse/JDK-8031085. hence have to do appendValue()
  private static final DateTimeFormatter MILLIS_INSTANT_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern(SECS_INSTANT_TIMESTAMP_FORMAT)
          .appendValue(ChronoField.MILLI_OF_SECOND, 3)
          .toFormatter()
          .withZone(ZONE_ID);

  /**
   * Copied mostly from {@link
   * org.apache.hudi.common.table.timeline.HoodieActiveTimeline#parseDateFromInstantTime(String)}
   * but forces the timestamp to use UTC unlike the Hudi code.
   *
   * @param timestamp input commit timestamp
   * @return timestamp parsed as Instant
   */
  static Instant parseFromInstantTime(String timestamp) {
    try {
      String timestampInMillis = timestamp;
      if (isSecondGranularity(timestamp)) {
        timestampInMillis = timestamp + "999";
      } else if (timestamp.length() > MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH) {
        timestampInMillis = timestamp.substring(0, MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH);
      }

      LocalDateTime dt = LocalDateTime.parse(timestampInMillis, MILLIS_INSTANT_TIME_FORMATTER);
      return dt.atZone(ZONE_ID).toInstant();
    } catch (DateTimeParseException ex) {
      throw new ParseException("Unable to parse date from commit timestamp: " + timestamp, ex);
    }
  }

  static String convertInstantToCommit(Instant instant) {
    LocalDateTime instantTime = instant.atZone(ZONE_ID).toLocalDateTime();
    return HoodieInstantTimeGenerator.getInstantFromTemporalAccessor(instantTime);
  }

  private static boolean isSecondGranularity(String instant) {
    return instant.length() == SECS_INSTANT_ID_LENGTH;
  }
}
