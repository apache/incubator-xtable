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
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.xtable.model.exception.ParseException;

/**
 * Metadata representing the state of a table sync process. This metadata is stored in the target
 * table's properties and is used to track the status of previous sync operation.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TableSyncMetadata {
  private static final int CURRENT_VERSION = 0;
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  /** Property name for the XTABLE metadata in the table metadata/properties */
  public static final String XTABLE_METADATA = "XTABLE_METADATA";

  Instant lastInstantSynced;
  List<Instant> instantsToConsiderForNextSync;
  int version;

  public static TableSyncMetadata of(
      Instant lastInstantSynced, List<Instant> instantsToConsiderForNextSync) {
    return new TableSyncMetadata(lastInstantSynced, instantsToConsiderForNextSync, CURRENT_VERSION);
  }

  public String toJson() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (IOException e) {
      throw new ParseException("Failed to serialize TableSyncMetadata", e);
    }
  }

  public static Optional<TableSyncMetadata> fromJson(String metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return Optional.empty();
    } else {
      try {
        TableSyncMetadata parsedMetadata = MAPPER.readValue(metadata, TableSyncMetadata.class);
        if (parsedMetadata.getLastInstantSynced() == null) {
          throw new ParseException("LastInstantSynced is required in TableSyncMetadata");
        }
        if (parsedMetadata.getVersion() > CURRENT_VERSION) {
          throw new ParseException(
              "Unable handle metadata version: "
                  + parsedMetadata.getVersion()
                  + " max supported version: "
                  + CURRENT_VERSION);
        }
        return Optional.of(parsedMetadata);
      } catch (IOException e) {
        throw new ParseException("Failed to deserialize TableSyncMetadata", e);
      }
    }
  }
}
