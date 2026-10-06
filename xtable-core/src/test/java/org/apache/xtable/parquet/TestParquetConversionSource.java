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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.TableFormat;

class TestParquetConversionSource {

  private static final String SCHEME = "xtable-test";
  private static final MessageType SCHEMA =
      new MessageType("message", Types.required(PrimitiveTypeName.INT32).named("col"));

  @TempDir java.nio.file.Path tempDir;

  /** The local file system served under a scheme that is neither the default nor file://. */
  public static class NonDefaultSchemeFileSystem extends RawLocalFileSystem {
    @Override
    public URI getUri() {
      return URI.create(SCHEME + ":///");
    }

    @Override
    public String getScheme() {
      return SCHEME;
    }

    // RawLocalFileSystem loads permissions through java.io.File, which only accepts file:// URIs
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
      return Arrays.stream(super.listStatus(path))
          .map(NonDefaultSchemeFileSystem::withoutPermissions)
          .toArray(FileStatus[]::new);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      return withoutPermissions(super.getFileStatus(path));
    }

    private static FileStatus withoutPermissions(FileStatus status) {
      return new FileStatus(
          status.getLen(),
          status.isDirectory(),
          status.getReplication(),
          status.getBlockSize(),
          status.getModificationTime(),
          status.getPath());
    }
  }

  @Test
  void getCurrentSnapshotListsFilesOnTheBasePathFileSystem() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs." + SCHEME + ".impl", NonDefaultSchemeFileSystem.class.getName());
    conf.setBoolean("fs." + SCHEME + ".impl.disable.cache", true);
    String basePath = SCHEME + "://" + tempDir.toAbsolutePath();
    Path file = new Path(basePath, "part-00000.parquet");
    GroupWriteSupport.setSchema(SCHEMA, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file).withConf(conf).build()) {
      for (int i = 0; i < 3; i++) {
        writer.write(factory.newGroup().append("col", i));
      }
    }

    SourceTable sourceTable =
        SourceTable.builder()
            .name("non_default_scheme")
            .formatName(TableFormat.PARQUET)
            .basePath(basePath)
            .additionalProperties(new Properties())
            .build();
    ParquetConversionSourceProvider provider = new ParquetConversionSourceProvider();
    provider.init(conf);
    InternalSnapshot snapshot =
        provider.getConversionSourceInstance(sourceTable).getCurrentSnapshot();

    List<InternalDataFile> dataFiles =
        snapshot.getPartitionedDataFiles().stream()
            .flatMap(group -> group.getDataFiles().stream())
            .collect(Collectors.toList());
    assertEquals(1, dataFiles.size());
    assertEquals(SCHEME, new Path(dataFiles.get(0).getPhysicalPath()).toUri().getScheme());
    assertEquals(3L, dataFiles.get(0).getRecordCount());
  }
}
