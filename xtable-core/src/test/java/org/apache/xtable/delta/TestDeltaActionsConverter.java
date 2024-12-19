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

import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;

import scala.Option;

class TestDeltaActionsConverter {

  @Test
  void extractDeletionVector() throws URISyntaxException {
    DeltaActionsConverter actionsConverter = DeltaActionsConverter.getInstance();

    int size = 123;
    long time = 234L;
    boolean dataChange = true;
    String stats = "";
    String filePath = "https://container.blob.core.windows.net/tablepath/file_path";
    Snapshot snapshot = Mockito.mock(Snapshot.class);
    DeltaLog deltaLog = Mockito.mock(DeltaLog.class);

    DeletionVectorDescriptor deletionVector = null;
    AddFile addFileAction =
        new AddFile(filePath, null, size, time, dataChange, stats, null, deletionVector);
    Assertions.assertNull(actionsConverter.extractDeletionVectorFile(snapshot, addFileAction));

    deletionVector =
        DeletionVectorDescriptor.onDiskWithAbsolutePath(
            filePath, size, 42, Option.empty(), Option.empty());

    addFileAction =
        new AddFile(filePath, null, size, time, dataChange, stats, null, deletionVector);

    Mockito.when(snapshot.deltaLog()).thenReturn(deltaLog);
    Mockito.when(deltaLog.dataPath())
        .thenReturn(new Path("https://container.blob.core.windows.net/tablepath"));
    Assertions.assertEquals(
        filePath, actionsConverter.extractDeletionVectorFile(snapshot, addFileAction));
  }
}
