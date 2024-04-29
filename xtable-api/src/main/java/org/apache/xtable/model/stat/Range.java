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
 
package org.apache.xtable.model.stat;

import java.util.Objects;

import lombok.Value;

import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.storage.InternalDataFile;

/**
 * Represents a range of values in the specified data type. Can represent a scalar value when the
 * {@link #minValue} and {@link #maxValue} are the same.
 *
 * <p>For the ranges stored in {@link InternalDataFile#getPartitionValues()}, the values will be
 * based off the {@link PartitionTransformType}. {@link PartitionTransformType#HOUR}, {@link
 * PartitionTransformType#DAY}, {@link PartitionTransformType#MONTH}, {@link
 * PartitionTransformType#YEAR} are all stored as a long representing a point in time as
 * milliseconds since epoch. {@link PartitionTransformType#VALUE} will match the rules below.
 *
 * <p>The minValue and maxValue object type will match the underlying InternalType (INT is integer,
 * DOUBLE is double, etc.) except for these cases:
 *
 * <ul>
 *   <li>{@link InternalType#TIMESTAMP} will be stored as a long represent millis or micros since
 *       epoch depending on the {@link InternalSchema.MetadataKey#TIMESTAMP_PRECISION}'s value
 *       ({@link InternalSchema.MetadataValue#MICROS} or {@link
 *       InternalSchema.MetadataValue#MILLIS})
 *   <li>{@link InternalType#TIMESTAMP_NTZ} will be stored as a long represent millis or micros
 *       since epoch depending on the {@link InternalSchema.MetadataKey#TIMESTAMP_PRECISION}'s value
 *       ({@link InternalSchema.MetadataValue#MICROS} or {@link
 *       InternalSchema.MetadataValue#MILLIS})
 *   <li>{@link InternalType#DATE} will be stored as an integer representing days since epoch
 *   <li>{@link InternalType#ENUM} will be stored as a string
 *   <li>{@link InternalType#FIXED} will be stored as a {@link java.nio.ByteBuffer}
 * </ul>
 *
 * @since 0.1
 */
@Value
public class Range {
  // define a reusable range for null values in case of sparse data
  private static final Range NULL_RANGE = new Range(null, null);

  Object minValue;
  Object maxValue;

  public static Range scalar(Object value) {
    if (value == null) {
      return NULL_RANGE;
    }
    return new Range(value, value);
  }

  public static Range vector(Object minValue, Object maxValue) {
    if (Objects.equals(minValue, maxValue)) {
      return scalar(minValue);
    }
    return new Range(minValue, maxValue);
  }
}
