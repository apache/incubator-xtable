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

import java.io.IOException;

import lombok.Builder;
import lombok.Value;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.xtable.model.exception.ParseException;

@Value
@Builder
public class SourceMetadata {
  // The source table snapshot identifier
  // Snapshot ID in Iceberg, version ID in Delta, and instant <timestamp_action> in Hudi
  String sourceIdentifier;
  // The table format of the source table
  String tableFormat;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static SourceMetadata fromJson(String json) throws ParseException {
    try {
      return MAPPER.readValue(json, SourceMetadata.class);
    } catch (IOException e) {
      throw new ParseException("Failed to deserialize SourceMetadata", e);
    }
  }

  public String toJson() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (IOException e) {
      throw new ParseException("Failed to serialize SourceMetadata", e);
    }
  }
}
