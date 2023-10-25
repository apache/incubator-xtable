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
 
package io.onetable.delta;

import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Builder
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TestDeltaHelper {
  private static final StructField[] COMMON_FIELDS =
      new StructField[] {
        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("firstName", DataTypes.StringType, true, Metadata.empty()),
        new StructField("lastName", DataTypes.StringType, true, Metadata.empty()),
        new StructField("gender", DataTypes.StringType, true, Metadata.empty()),
        new StructField("birthDate", DataTypes.TimestampType, true, Metadata.empty())
      };
  private static final StructType PERSON_SCHEMA_PARTITIONED =
      new StructType(
          appendFields(
              COMMON_FIELDS,
              new StructField("yearOfBirth", DataTypes.IntegerType, true, Metadata.empty())));
  private static final StructType PERSON_SCHEMA_NON_PARTITIONED = new StructType(COMMON_FIELDS);

  // Until Delta 2.4 even generated columns should be provided values.
  private static final String COMMON_SQL_SELECT_FIELDS =
      "SELECT %d AS id, "
          + "'%s' AS firstName, "
          + "'%s' AS lastName, "
          + "'%s' AS gender, "
          + "timestamp('%s') AS birthDate";
  private static final String SQL_SELECT_TEMPLATE_PARTITIONED =
      COMMON_SQL_SELECT_FIELDS + ", year(timestamp('%s')) AS yearOfBirth";
  private static final String SQL_SELECT_TEMPLATE_NON_PARTITIONED = COMMON_SQL_SELECT_FIELDS;

  private static final String SQL_CREATE_TABLE_TEMPLATE_PARTITIONED =
      "CREATE TABLE `%s` ("
          + "    id INT, "
          + "    firstName STRING, "
          + "    lastName STRING, "
          + "    gender STRING, "
          + "    birthDate TIMESTAMP, "
          + "    yearOfBirth INT "
          + ") USING DELTA "
          + "PARTITIONED BY (yearOfBirth) "
          + "LOCATION '%s'";

  private static final String SQL_CREATE_TABLE_TEMPLATE_NON_PARTITIONED =
      "CREATE TABLE `%s` ("
          + "    id INT, "
          + "    firstName STRING, "
          + "    lastName STRING, "
          + "    gender STRING, "
          + "    birthDate TIMESTAMP "
          + ") USING DELTA "
          + "LOCATION '%s'";

  private static StructField[] appendFields(
      StructField[] originalFields, StructField... newFields) {
    return Stream.concat(Stream.of(originalFields), Stream.of(newFields))
        .toArray(StructField[]::new);
  }

  private final StructType structSchema;
  private final String selectForInserts;
  private final String createSqlStr;

  public static TestDeltaHelper createTestDataHelper(boolean isPartitioned) {
    if (isPartitioned) {
      return TestDeltaHelper.builder()
          .createSqlStr(SQL_CREATE_TABLE_TEMPLATE_PARTITIONED)
          .selectForInserts(SQL_SELECT_TEMPLATE_PARTITIONED)
          .structSchema(PERSON_SCHEMA_PARTITIONED)
          .build();
    } else {
      return TestDeltaHelper.builder()
          .createSqlStr(SQL_CREATE_TABLE_TEMPLATE_NON_PARTITIONED)
          .selectForInserts(SQL_SELECT_TEMPLATE_NON_PARTITIONED)
          .structSchema(PERSON_SCHEMA_NON_PARTITIONED)
          .build();
    }
  }

  public String generateSelectWithAdditionalColumn() {
    return this.selectForInserts + ", '%s' AS street";
  }
}
