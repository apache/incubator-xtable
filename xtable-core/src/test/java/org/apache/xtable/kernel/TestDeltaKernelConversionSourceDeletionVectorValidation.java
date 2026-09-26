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
 
package org.apache.xtable.kernel;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;

import org.apache.xtable.delta.DeltaDeletionVectorHandler;

class TestDeltaKernelConversionSourceDeletionVectorValidation {

  @Test
  void skipsActiveFileListingWhenDeletionVectorsAreUnsupported() {
    DeltaKernelDataFileExtractor dataFileExtractor = mock(DeltaKernelDataFileExtractor.class);
    DeltaDeletionVectorHandler deletionVectorHandler = mock(DeltaDeletionVectorHandler.class);
    Engine engine = mock(Engine.class);
    Table table = mock(Table.class);
    SnapshotImpl snapshot = mock(SnapshotImpl.class);
    Protocol protocol = mock(Protocol.class);
    when(deletionVectorHandler.isRejecting()).thenReturn(true);
    when(snapshot.getProtocol()).thenReturn(protocol);
    when(protocol.supportsFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE)).thenReturn(false);

    DeltaKernelConversionSource source =
        DeltaKernelConversionSource.builder()
            .dataFileExtractor(dataFileExtractor)
            .deletionVectorHandler(deletionVectorHandler)
            .engine(engine)
            .build();
    source.validateActiveDeletionVectors(snapshot, table);

    verify(dataFileExtractor, never())
        .validateDeletionVectors(snapshot, table, engine, deletionVectorHandler);
  }

  @Test
  void listsActiveFilesWhenDeletionVectorsAreSupported() {
    DeltaKernelDataFileExtractor dataFileExtractor = mock(DeltaKernelDataFileExtractor.class);
    DeltaDeletionVectorHandler deletionVectorHandler = mock(DeltaDeletionVectorHandler.class);
    Engine engine = mock(Engine.class);
    Table table = mock(Table.class);
    SnapshotImpl snapshot = mock(SnapshotImpl.class);
    Protocol protocol = mock(Protocol.class);
    when(deletionVectorHandler.isRejecting()).thenReturn(true);
    when(snapshot.getProtocol()).thenReturn(protocol);
    when(protocol.supportsFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE)).thenReturn(true);

    DeltaKernelConversionSource source =
        DeltaKernelConversionSource.builder()
            .dataFileExtractor(dataFileExtractor)
            .deletionVectorHandler(deletionVectorHandler)
            .engine(engine)
            .build();
    source.validateActiveDeletionVectors(snapshot, table);

    verify(dataFileExtractor)
        .validateDeletionVectors(snapshot, table, engine, deletionVectorHandler);
  }

  @Test
  void skipsActiveFileListingWhenDeletionVectorsAreAllowed() {
    DeltaKernelDataFileExtractor dataFileExtractor = mock(DeltaKernelDataFileExtractor.class);
    DeltaDeletionVectorHandler deletionVectorHandler = mock(DeltaDeletionVectorHandler.class);
    Engine engine = mock(Engine.class);
    Table table = mock(Table.class);
    SnapshotImpl snapshot = mock(SnapshotImpl.class);
    when(deletionVectorHandler.isRejecting()).thenReturn(false);

    DeltaKernelConversionSource source =
        DeltaKernelConversionSource.builder()
            .dataFileExtractor(dataFileExtractor)
            .deletionVectorHandler(deletionVectorHandler)
            .engine(engine)
            .build();
    source.validateActiveDeletionVectors(snapshot, table);

    verify(dataFileExtractor, never())
        .validateDeletionVectors(snapshot, table, engine, deletionVectorHandler);
  }
}
