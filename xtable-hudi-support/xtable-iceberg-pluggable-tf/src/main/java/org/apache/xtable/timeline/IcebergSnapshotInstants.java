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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.SneakyThrows;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.xtable.model.metadata.TableSyncMetadata;

/** Decodes the Hudi instant that each Iceberg snapshot records in its summary. */
public final class IcebergSnapshotInstants {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private IcebergSnapshotInstants() {}

  /**
   * The ancestry of the current snapshot, oldest first, or an empty list when the table has no
   * snapshot. Only the ancestry is authoritative for the Hudi timeline: other retained snapshots
   * are not reachable by any reader.
   */
  public static List<Snapshot> ancestorsOldestFirst(Table table) {
    List<Snapshot> ancestors = new ArrayList<>();
    SnapshotUtil.currentAncestors(table).forEach(ancestors::add);
    Collections.reverse(ancestors);
    return ancestors;
  }

  @SneakyThrows
  public static HoodieInstant recordedInstant(
      Snapshot snapshot, InstantGenerator instantGenerator) {
    TableSyncMetadata syncMetadata =
        TableSyncMetadata.fromJson(snapshot.summary().get(TableSyncMetadata.XTABLE_METADATA))
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Iceberg snapshot %d carries no XTable metadata, so it was not written "
                                + "by the Hudi table format",
                            snapshot.snapshotId())));
    return InstantDTO.toInstant(
        MAPPER.readValue(syncMetadata.getLatestTableOperationIdentifier(), InstantDTO.class),
        instantGenerator);
  }
}
