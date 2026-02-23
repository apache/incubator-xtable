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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;

class TestDeltaDataFileUpdatesExtractor {

  @Test
  void shouldPersistNumRecordsZeroWhenRecordCountIsZeroAndColumnStatsAreEmpty() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("root")
            .dataType(InternalType.RECORD)
            .fields(
                Collections.singletonList(
                    InternalField.builder()
                        .name("id")
                        .schema(
                            InternalSchema.builder()
                                .name("id")
                                .dataType(InternalType.INT)
                                .isNullable(true)
                                .build())
                        .build()))
            .isNullable(false)
            .build();

    InternalDataFile dataFile =
        InternalDataFile.builder()
            .physicalPath("s3://bucket/table/path/file-1.parquet")
            .fileFormat(FileFormat.APACHE_PARQUET)
            .fileSizeBytes(128)
            .recordCount(0)
            .columnStats(Collections.emptyList())
            .lastModified(1234L)
            .build();

    InternalFilesDiff diff =
        InternalFilesDiff.builder()
            .fileAdded(dataFile)
            .filesRemoved(Collections.emptySet())
            .build();

    DeltaDataFileUpdatesExtractor extractor = DeltaDataFileUpdatesExtractor.builder().build();
    Seq<Action> actions = extractor.applyDiff(diff, schema, "s3://bucket/table/path");
    AddFile addFile =
        JavaConverters.seqAsJavaList(actions).stream()
            .filter(AddFile.class::isInstance)
            .map(AddFile.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected AddFile action"));

    assertNotNull(addFile.stats());
    assertTrue(addFile.stats().contains("\"numRecords\":0"));
  }
}
