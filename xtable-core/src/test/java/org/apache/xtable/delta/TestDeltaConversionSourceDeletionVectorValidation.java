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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;

class TestDeltaConversionSourceDeletionVectorValidation {

  @Test
  void skipsActiveFileListingWhenDeletionVectorsAreUnsupported() {
    Snapshot snapshot = mock(Snapshot.class);
    DeltaDeletionVectorHandler deletionVectorHandler = mock(DeltaDeletionVectorHandler.class);
    when(deletionVectorHandler.isRejecting()).thenReturn(true);
    when(snapshot.deletionVectorsSupported()).thenReturn(false);

    DeltaConversionSource.builder()
        .deletionVectorHandler(deletionVectorHandler)
        .build()
        .validateActiveDeletionVectors(snapshot);

    verify(snapshot, never()).allFiles();
  }

  @Test
  void listsActiveFilesWhenDeletionVectorsAreSupported() {
    Snapshot snapshot = mock(Snapshot.class);
    DeltaDeletionVectorHandler deletionVectorHandler = mock(DeltaDeletionVectorHandler.class);
    @SuppressWarnings("unchecked")
    Dataset<AddFile> activeFiles = mock(Dataset.class);
    when(deletionVectorHandler.isRejecting()).thenReturn(true);
    when(snapshot.deletionVectorsSupported()).thenReturn(true);
    when(snapshot.allFiles()).thenReturn(activeFiles);
    when(activeFiles.toLocalIterator()).thenReturn(Collections.emptyIterator());

    DeltaConversionSource.builder()
        .deletionVectorHandler(deletionVectorHandler)
        .build()
        .validateActiveDeletionVectors(snapshot);

    verify(activeFiles).toLocalIterator();
  }

  @Test
  void skipsActiveFileListingWhenDeletionVectorsAreAllowed() {
    Snapshot snapshot = mock(Snapshot.class);
    DeltaDeletionVectorHandler deletionVectorHandler = mock(DeltaDeletionVectorHandler.class);
    when(deletionVectorHandler.isRejecting()).thenReturn(false);

    DeltaConversionSource.builder()
        .deletionVectorHandler(deletionVectorHandler)
        .build()
        .validateActiveDeletionVectors(snapshot);

    verify(snapshot, never()).allFiles();
  }
}
