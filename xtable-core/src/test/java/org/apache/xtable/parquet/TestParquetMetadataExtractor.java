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
 
package org.apache.xtable.parquet;

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParquetMetadataExtractor {

  private static final MessageType SCHEMA =
      new MessageType("message", Types.required(PrimitiveTypeName.INT32).named("col"));

  private final Configuration conf = new Configuration();
  private final ParquetMetadataExtractor metadataExtractor = ParquetMetadataExtractor.getInstance();
  @TempDir java.nio.file.Path tempDir;

  @Test
  void getRowCountSingleRowGroup() throws IOException {
    Path path = new Path(tempDir.resolve("single-rg.parquet").toUri());
    GroupWriteSupport.setSchema(SCHEMA, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withConf(conf).build()) {
      for (int i = 0; i < 3; i++) {
        writer.write(factory.newGroup().append("col", i));
      }
    }

    assertEquals(
        3L, metadataExtractor.getRowCount(metadataExtractor.readParquetMetadata(conf, path)));
  }

  @Test
  void getRowCountSumsAllRowGroups() throws IOException {
    Path path = new Path(tempDir.resolve("multi-rg.parquet").toUri());
    ColumnDescriptor col = SCHEMA.getColumns().get(0);
    ParquetFileWriter writer = new ParquetFileWriter(conf, SCHEMA, path);
    writer.start();
    writer.startBlock(10);
    writer.startColumn(col, 10, CompressionCodecName.UNCOMPRESSED);
    writer.writeDataPage(10, 4, BytesInput.fromInt(0), BIT_PACKED, BIT_PACKED, PLAIN);
    writer.endColumn();
    writer.endBlock();
    writer.startBlock(5);
    writer.startColumn(col, 5, CompressionCodecName.UNCOMPRESSED);
    writer.writeDataPage(5, 4, BytesInput.fromInt(0), BIT_PACKED, BIT_PACKED, PLAIN);
    writer.endColumn();
    writer.endBlock();
    writer.end(new HashMap<>());

    assertEquals(
        15L, metadataExtractor.getRowCount(metadataExtractor.readParquetMetadata(conf, path)));
  }
}
