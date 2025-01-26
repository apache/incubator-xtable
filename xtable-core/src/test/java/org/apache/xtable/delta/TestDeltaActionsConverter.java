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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray;
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArrayFormat;

import scala.Option;

import org.apache.xtable.model.storage.InternalDeletionVector;

class TestDeltaActionsConverter {

  private final String basePath = "https://container.blob.core.windows.net/tablepath/";
  private final int size = 372;
  private final long time = 376;
  private final boolean dataChange = true;
  private final String stats = "";
  private final int cardinality = 42;
  private final int offset = 634;

  @Test
  void extractMissingDeletionVector() {
    DeltaActionsConverter actionsConverter = DeltaActionsConverter.getInstance();

    String filePath = basePath + "file_path";
    Snapshot snapshot = Mockito.mock(Snapshot.class);

    DeletionVectorDescriptor deletionVector = null;
    AddFile addFileAction =
        new AddFile(filePath, null, size, time, dataChange, stats, null, deletionVector);
    InternalDeletionVector internaldeletionVector =
        actionsConverter.extractDeletionVector(snapshot, addFileAction);
    assertNull(internaldeletionVector);
  }

  @Test
  void extractDeletionVectorInFileAbsolutePath() {
    DeltaActionsConverter actionsConverter = spy(DeltaActionsConverter.getInstance());

    String dataFilePath = "data_file";
    String deleteFilePath = "https://container.blob.core.windows.net/tablepath/delete_file";
    Snapshot snapshot = Mockito.mock(Snapshot.class);

    DeletionVectorDescriptor deletionVector =
        DeletionVectorDescriptor.onDiskWithAbsolutePath(
            deleteFilePath, size, cardinality, Option.apply(offset), Option.empty());

    AddFile addFileAction =
        new AddFile(dataFilePath, null, size, time, dataChange, stats, null, deletionVector);

    Configuration conf = new Configuration();
    DeltaLog deltaLog = Mockito.mock(DeltaLog.class);
    when(snapshot.deltaLog()).thenReturn(deltaLog);
    when(deltaLog.dataPath()).thenReturn(new Path(basePath));
    when(deltaLog.newDeltaHadoopConf()).thenReturn(conf);

    long[] ordinals = {45, 78, 98};
    Mockito.doReturn(ordinals)
        .when(actionsConverter)
        .parseOrdinalFile(conf, new Path(deleteFilePath), size, offset);

    InternalDeletionVector internaldeletionVector =
        actionsConverter.extractDeletionVector(snapshot, addFileAction);
    assertNotNull(internaldeletionVector);
    assertEquals(basePath + dataFilePath, internaldeletionVector.dataFilePath());
    assertEquals(deleteFilePath, internaldeletionVector.sourceDeletionVectorFilePath());
    assertEquals(offset, internaldeletionVector.offset());
    assertEquals(cardinality, internaldeletionVector.countRecordsDeleted());
    assertEquals(size, internaldeletionVector.size());
    assertNull(internaldeletionVector.binaryRepresentation());

    Iterator<Long> iterator = internaldeletionVector.ordinalsIterator();
    Arrays.stream(ordinals).forEach(o -> assertEquals(o, iterator.next()));
    assertFalse(iterator.hasNext());
  }

  @Test
  void extractDeletionVectorInFileRelativePath() {
    DeltaActionsConverter actionsConverter = spy(DeltaActionsConverter.getInstance());

    String dataFilePath = "data_file";
    UUID deleteFileId = UUID.randomUUID();
    String deleteFilePath = basePath + "deletion_vector_" + deleteFileId + ".bin";
    Snapshot snapshot = Mockito.mock(Snapshot.class);

    DeletionVectorDescriptor deletionVector =
        DeletionVectorDescriptor.onDiskWithRelativePath(
            deleteFileId, "", size, cardinality, Option.apply(offset), Option.empty());

    AddFile addFileAction =
        new AddFile(dataFilePath, null, size, time, dataChange, stats, null, deletionVector);

    Configuration conf = new Configuration();
    DeltaLog deltaLog = Mockito.mock(DeltaLog.class);
    when(snapshot.deltaLog()).thenReturn(deltaLog);
    when(deltaLog.dataPath()).thenReturn(new Path(basePath));
    when(deltaLog.newDeltaHadoopConf()).thenReturn(conf);

    long[] ordinals = {45, 78, 98};
    Mockito.doReturn(ordinals)
        .when(actionsConverter)
        .parseOrdinalFile(conf, new Path(deleteFilePath), size, offset);

    InternalDeletionVector internaldeletionVector =
        actionsConverter.extractDeletionVector(snapshot, addFileAction);
    assertNotNull(internaldeletionVector);
    assertEquals(basePath + dataFilePath, internaldeletionVector.dataFilePath());
    assertEquals(deleteFilePath, internaldeletionVector.sourceDeletionVectorFilePath());
    assertEquals(offset, internaldeletionVector.offset());
    assertEquals(cardinality, internaldeletionVector.countRecordsDeleted());
    assertEquals(size, internaldeletionVector.size());
    assertNull(internaldeletionVector.binaryRepresentation());

    Iterator<Long> iterator = internaldeletionVector.ordinalsIterator();
    Arrays.stream(ordinals).forEach(o -> assertEquals(o, iterator.next()));
    assertFalse(iterator.hasNext());
  }

  @Test
  void extractInlineDeletionVector() {
    DeltaActionsConverter actionsConverter = spy(DeltaActionsConverter.getInstance());

    String dataFilePath = "data_file";
    Snapshot snapshot = Mockito.mock(Snapshot.class);

    long[] ordinals = {45, 78, 98};
    RoaringBitmapArray rbm = new RoaringBitmapArray();
    Arrays.stream(ordinals).forEach(rbm::add);
    byte[] bytes = rbm.serializeAsByteArray(RoaringBitmapArrayFormat.Portable());

    DeletionVectorDescriptor deletionVector =
        DeletionVectorDescriptor.inlineInLog(bytes, cardinality);

    AddFile addFileAction =
        new AddFile(dataFilePath, null, size, time, dataChange, stats, null, deletionVector);

    DeltaLog deltaLog = Mockito.mock(DeltaLog.class);
    when(snapshot.deltaLog()).thenReturn(deltaLog);
    when(deltaLog.dataPath()).thenReturn(new Path(basePath));

    InternalDeletionVector internaldeletionVector =
        actionsConverter.extractDeletionVector(snapshot, addFileAction);
    assertNotNull(internaldeletionVector);
    assertEquals(basePath + dataFilePath, internaldeletionVector.dataFilePath());
    assertArrayEquals(bytes, internaldeletionVector.binaryRepresentation());
    assertEquals(cardinality, internaldeletionVector.countRecordsDeleted());
    assertEquals(bytes.length, internaldeletionVector.size());
    assertNull(internaldeletionVector.sourceDeletionVectorFilePath());
    assertEquals(0, internaldeletionVector.offset());

    Iterator<Long> iterator = internaldeletionVector.ordinalsIterator();
    Arrays.stream(ordinals).forEach(o -> assertEquals(o, iterator.next()));
    assertFalse(iterator.hasNext());
  }
}
