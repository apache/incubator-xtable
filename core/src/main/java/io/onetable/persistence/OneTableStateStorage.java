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
 
package io.onetable.persistence;

import static io.onetable.constants.OneTableConstants.NUM_ARCHIVED_SYNCS_RESULTS;
import static io.onetable.constants.OneTableConstants.ONETABLE_META_DIR;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.onetable.constants.OneTableConstants;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncResult;

/**
 * Represents the storage for {@link OneTableState}. It stores the state in the folder {@link
 * OneTableConstants#ONETABLE_META_DIR}. It also archives the older sync results.
 */
public class OneTableStateStorage {
  private static final ObjectMapper MAPPER =
      new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
  private final Configuration conf;
  private final boolean enableArchive;

  public OneTableStateStorage(Configuration conf, boolean enableArchive) {
    this.conf = conf;
    this.enableArchive = enableArchive;
  }

  public void write(OneTableState state) throws IOException {
    String tableBasePath = state.getTable().getBasePath();
    if (enableArchive) {
      for (TableFormat tableFormat : state.getTableFormatsToSync()) {
        SyncResult currentSyncResult = state.getLastSyncResult().get(tableFormat);
        String syncFileName = String.format("%s.sync", tableFormat.name().toLowerCase(Locale.ROOT));
        Path syncFilePath =
            new Path(String.format("%s/%s/%s", tableBasePath, ONETABLE_META_DIR, syncFileName));
        String syncArchivedFileName =
            String.format("%s.sync.archive", tableFormat.name().toLowerCase(Locale.ROOT));
        Path syncArchivedFilePath =
            new Path(
                String.format("%s/%s/%s", tableBasePath, ONETABLE_META_DIR, syncArchivedFileName));

        // Read last archived
        List<SyncResult> archivedSyncResults = readSyncResults(syncArchivedFilePath);
        List<SyncResult> archivedSyncResultsSortedByStartTime =
            archivedSyncResults.stream()
                .sorted(Comparator.comparing(SyncResult::getSyncStartTime))
                .collect(Collectors.toList());
        archivedSyncResultsSortedByStartTime.add(currentSyncResult);
        if (archivedSyncResultsSortedByStartTime.size() > NUM_ARCHIVED_SYNCS_RESULTS) {
          archivedSyncResultsSortedByStartTime =
              archivedSyncResultsSortedByStartTime.subList(
                  archivedSyncResultsSortedByStartTime.size() - NUM_ARCHIVED_SYNCS_RESULTS,
                  archivedSyncResultsSortedByStartTime.size());
        }
        // Write tableFormat.sync and tableFormat.sync.archive files.
        writeSyncResults(syncFilePath, Collections.singletonList(currentSyncResult));
        writeSyncResults(syncArchivedFilePath, archivedSyncResultsSortedByStartTime);
      }
    }
    // Write .onetable.state to read it later for incremental syncs.
    Path targetFsPath =
        new Path(String.format("%s/%s/%s", tableBasePath, ONETABLE_META_DIR, "state"));
    writeOneTableState(targetFsPath, state);
  }

  public Optional<OneTableState> read(String tableBasePath) throws IOException {
    Path targetFsPath =
        new Path(String.format("%s/%s/%s", tableBasePath, ONETABLE_META_DIR, "state"));
    return readOneTableState(targetFsPath);
  }

  private void writeSyncResults(Path targetFsPath, List<SyncResult> syncResultList)
      throws IOException {
    FileSystem fs = targetFsPath.getFileSystem(conf);
    try (FSDataOutputStream fsDataOutputStream = fs.create(targetFsPath);
        BufferedWriter bufferedWriter =
            new BufferedWriter(
                new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))) {
      // Persisted as new-line JSON serialised format.
      for (SyncResult syncResult : syncResultList) {
        bufferedWriter.write(MAPPER.writeValueAsString(syncResult) + "\n");
      }
    }
  }

  private void writeOneTableState(Path targetFsPath, OneTableState state) throws IOException {
    FileSystem fs = targetFsPath.getFileSystem(conf);
    try (FSDataOutputStream fsDataOutputStream = fs.create(targetFsPath);
        BufferedWriter bufferedWriter =
            new BufferedWriter(
                new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))) {
      bufferedWriter.write(MAPPER.writeValueAsString(state) + "\n");
    }
  }

  private Optional<OneTableState> readOneTableState(Path targetFsPath) throws IOException {
    FileSystem fs = targetFsPath.getFileSystem(conf);
    if (fs.exists(targetFsPath)) {
      try (FSDataInputStream fsDataInputStream = fs.open(targetFsPath);
          BufferedReader bufferedReader =
              new BufferedReader(
                  new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {
        String readLine = bufferedReader.readLine();
        return Optional.of(MAPPER.readValue(readLine, OneTableState.class));
      }
    }
    return Optional.empty();
  }

  private List<SyncResult> readSyncResults(Path targetFsPath) throws IOException {
    FileSystem fs = targetFsPath.getFileSystem(conf);
    List<SyncResult> syncResultList = new ArrayList<>();
    if (fs.exists(targetFsPath)) {
      try (FSDataInputStream fsDataInputStream = fs.open(targetFsPath);
          BufferedReader bufferedReader =
              new BufferedReader(
                  new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {
        String readLine = bufferedReader.readLine();
        while (readLine != null) {
          syncResultList.add(MAPPER.readValue(readLine, SyncResult.class));
          readLine = bufferedReader.readLine();
        }
      }
    }
    return syncResultList;
  }
}
