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
 
package org.apache.xtable.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import org.apache.xtable.model.storage.InternalDeletionVector;

public class TestIcebergDeleteVectorConverter {
  @TempDir private Path tempDirPath;

  @Test
  public void x() throws Exception {
    Path tablePath = Paths.get(tempDirPath.toString(), "table");
    Path dataFilePath = Paths.get(tablePath.toString(), "part=1", "data_1.parquet");

    List<Long> ordinals = Arrays.asList(1L, 10L, 12L, 13L, 20L);

    InternalDeletionVector vector =
        InternalDeletionVector.builder()
            .physicalPath("")
            .dataFilePath(dataFilePath.toString())
            .ordinalsSupplier(ordinals::iterator)
            .build();

    IcebergDeleteVectorConverter converter =
        IcebergDeleteVectorConverter.builder().directoryPath(tempDirPath).build();

    HadoopFileIO fileIO = new HadoopFileIO(new Configuration());
    DeleteFile deleteFile = converter.toIceberg(fileIO, vector);
    assertNotNull(deleteFile);
    assertNotNull(deleteFile.path());
    assertTrue(Files.exists(Paths.get(deleteFile.path().toString()), LinkOption.NOFOLLOW_LINKS));

    // assert delete file exists
    CharSequence deleteFilePath = deleteFile.path();
    Schema deleteSchema = DeleteSchemaUtil.posDeleteSchema(null);

    InputFile deleteInputFile = org.apache.iceberg.Files.localInput(deleteFilePath.toString());
    try (CloseableIterable<Record> reader =
        Parquet.read(deleteInputFile)
            .project(deleteSchema)
            .createReaderFunc(schema -> GenericParquetReaders.buildReader(deleteSchema, schema))
            .build()) {
      ArrayList<Record> deletedRecords = Lists.newArrayList(reader);
      for (int i = 0; i < ordinals.size(); i++) {
        Record record = deletedRecords.get(i);
        assertEquals(2, record.size());
        assertEquals(record.get(0), dataFilePath.toString());
        assertEquals(record.get(1), ordinals.get(i));
      }
    }
  }
}
