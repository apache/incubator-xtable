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
 
package org.apache.xtable.timeline;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.xtable.iceberg.IcebergTableManager;
import org.apache.xtable.model.metadata.TableSyncMetadata;

public class IcebergActiveTimeline extends ActiveTimelineV2 {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  public IcebergActiveTimeline(
      HoodieTableMetaClient metaClient,
      Set<String> includedExtensions,
      boolean applyLayoutFilters) {
    this.setInstants(getInstantsFromFileSystem(metaClient, includedExtensions, applyLayoutFilters));
    this.metaClient = metaClient;
  }

  public IcebergActiveTimeline(HoodieTableMetaClient metaClient) {
    this(metaClient, Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE), true);
  }

  public IcebergActiveTimeline(HoodieTableMetaClient metaClient, boolean applyLayoutFilters) {
    this(
        metaClient,
        Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE),
        applyLayoutFilters);
  }

  public IcebergActiveTimeline() {}

  @SneakyThrows
  protected List<HoodieInstant> getInstantsFromFileSystem(
      HoodieTableMetaClient metaClient,
      Set<String> includedExtensions,
      boolean applyLayoutFilters) {
    List<HoodieInstant> instantsFromHoodieTimeline =
        super.getInstantsFromFileSystem(metaClient, includedExtensions, applyLayoutFilters);
    IcebergTableManager icebergTableManager =
        IcebergTableManager.of((Configuration) metaClient.getStorageConf().unwrap());
    TableIdentifier tableIdentifier =
        TableIdentifier.of(metaClient.getTableConfig().getTableName());
    if (!icebergTableManager.tableExists(
        null, tableIdentifier, metaClient.getBasePath().toString())) {
      return Collections.emptyList();
    }
    Table icebergTable =
        icebergTableManager.getTable(null, tableIdentifier, metaClient.getBasePath().toString());
    Map<String, HoodieInstant> instantsFromIceberg = new HashMap<>();
    for (Snapshot snapshot : icebergTable.snapshots()) {
      TableSyncMetadata syncMetadata =
          TableSyncMetadata.fromJson(snapshot.summary().get(TableSyncMetadata.XTABLE_METADATA))
              .get();
      HoodieInstant hoodieInstant =
          InstantDTO.toInstant(
              MAPPER.readValue(syncMetadata.getLatestTableOperationId(), InstantDTO.class),
              metaClient.getInstantGenerator());
      instantsFromIceberg.put(hoodieInstant.requestedTime(), hoodieInstant);
    }
    List<HoodieInstant> inflightInstantsInIceberg =
        instantsFromHoodieTimeline.stream()
            .filter(
                hoodieInstant -> !instantsFromIceberg.containsKey(hoodieInstant.requestedTime()))
            .map(
                instant -> {
                  if (instant.isCompleted()) {
                    return new HoodieInstant(
                        HoodieInstant.State.INFLIGHT,
                        instant.getAction(),
                        instant.requestedTime(),
                        instant.getCompletionTime(),
                        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
                  }
                  return instant;
                })
            .collect(Collectors.toList());
    List<HoodieInstant> completedInstantsInIceberg =
        instantsFromIceberg.values().stream()
            .filter(instantsFromHoodieTimeline::contains)
            .collect(Collectors.toList());
    return Stream.concat(completedInstantsInIceberg.stream(), inflightInstantsInIceberg.stream())
        .sorted(InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)
        .collect(Collectors.toList());
  }
}
