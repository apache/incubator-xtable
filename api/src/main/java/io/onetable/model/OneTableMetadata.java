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
 
package io.onetable.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(staticName = "of")
@Value
public class OneTableMetadata {
  /**
   * Property name for the lastInstantSynced field from SyncResult, used for persisting
   * lastInstantSynced in the table metadata/properties
   */
  private static final String ONETABLE_LAST_INSTANT_SYNCED_PROP = "ONETABLE_LAST_INSTANT_SYNCED";

  Instant lastInstantSynced;

  public Map<String, String> asMap() {
    Map<String, String> map = new HashMap<>();
    map.put(ONETABLE_LAST_INSTANT_SYNCED_PROP, lastInstantSynced.toString());
    return map;
  }

  public static Optional<OneTableMetadata> fromMap(Map<String, String> properties) {
    if (properties != null && properties.containsKey(ONETABLE_LAST_INSTANT_SYNCED_PROP)) {
      return Optional.ofNullable(
          OneTableMetadata.of(Instant.parse(properties.get(ONETABLE_LAST_INSTANT_SYNCED_PROP))));
    }
    return Optional.empty();
  }
}
