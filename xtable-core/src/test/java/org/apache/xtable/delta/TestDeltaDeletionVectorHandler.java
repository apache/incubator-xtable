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
 
package org.apache.xtable.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.storage.InternalDataFile;

public class TestDeltaDeletionVectorHandler {
  private static final String DATA_FILE_PATH = "file:///table/part-0001.parquet";

  @Test
  public void rejectsDeletionVectorsByDefault() {
    DeltaDeletionVectorHandler handler = new DeltaDeletionVectorHandler(false);

    NotSupportedException exception =
        assertThrows(
            NotSupportedException.class, () -> handler.onDeletionVectorFound(DATA_FILE_PATH));

    assertTrue(exception.getMessage().contains(DATA_FILE_PATH));
    assertTrue(
        exception
            .getMessage()
            .contains(DeltaConversionSourceConfig.ALLOW_UNSUPPORTED_DELETION_VECTORS));
  }

  @Test
  public void warnsAndContinuesWhenExplicitlyAllowed() {
    List<String> warnings = new ArrayList<>();
    DeltaDeletionVectorHandler handler = new DeltaDeletionVectorHandler(true, warnings::add);

    handler.onDeletionVectorFound(DATA_FILE_PATH);

    assertEquals(1, warnings.size());
    assertTrue(warnings.get(0).contains(DATA_FILE_PATH));
    assertTrue(warnings.get(0).contains("may contain rows that were deleted"));
  }

  @Test
  public void removesMatchingDeletionVectorFileChanges() {
    Map<String, InternalDataFile> addedFiles = new HashMap<>();
    Map<String, InternalDataFile> removedFiles = new HashMap<>();
    Set<String> dataFilesWithDeletionVectors = new HashSet<>();
    addedFiles.put(DATA_FILE_PATH, mock(InternalDataFile.class));
    removedFiles.put(DATA_FILE_PATH, mock(InternalDataFile.class));
    dataFilesWithDeletionVectors.add(DATA_FILE_PATH);

    DeltaDeletionVectorHandler.removeDeletionVectorFileChanges(
        addedFiles, removedFiles, dataFilesWithDeletionVectors);

    assertTrue(addedFiles.isEmpty());
    assertTrue(removedFiles.isEmpty());
  }

  @Test
  public void preservesUnmatchedDeletionVectorFileChanges() {
    Map<String, InternalDataFile> addedFiles = new HashMap<>();
    Map<String, InternalDataFile> removedFiles = new HashMap<>();
    Set<String> dataFilesWithDeletionVectors = new HashSet<>();
    addedFiles.put(DATA_FILE_PATH, mock(InternalDataFile.class));
    dataFilesWithDeletionVectors.add(DATA_FILE_PATH);

    DeltaDeletionVectorHandler.removeDeletionVectorFileChanges(
        addedFiles, removedFiles, dataFilesWithDeletionVectors);

    assertTrue(addedFiles.containsKey(DATA_FILE_PATH));
    assertFalse(removedFiles.containsKey(DATA_FILE_PATH));
  }
}
