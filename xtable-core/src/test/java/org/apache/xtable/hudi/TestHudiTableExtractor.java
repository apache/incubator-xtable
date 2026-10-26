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
 
package org.apache.xtable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;

/**
 * The table description derived from a commit's own metadata has to agree with the one {@link
 * org.apache.hudi.common.table.TableSchemaResolver} derives from the timeline, since a table's
 * snapshots are written by both paths.
 */
class TestHudiTableExtractor {
  private static final Schema WRITER_SCHEMA =
      SchemaBuilder.record("record")
          .fields()
          .requiredString("key")
          .requiredInt("value")
          .endRecord();

  @TempDir Path tempDir;

  @Test
  void metaFieldsFollowTheTableConfig() throws Exception {
    InternalTable withMetaFields = describe(newMetaClient("with_meta_fields", true, null), null);
    assertTrue(hasField(withMetaFields, HoodieRecord.COMMIT_TIME_METADATA_FIELD));

    InternalTable withoutMetaFields =
        describe(newMetaClient("without_meta_fields", false, null), null);
    assertFalse(
        hasField(withoutMetaFields, HoodieRecord.COMMIT_TIME_METADATA_FIELD),
        "a table that does not populate meta fields must not describe them");
    assertTrue(hasField(withoutMetaFields, "value"));
  }

  @Test
  void droppedPartitionColumnsAreAppended() throws Exception {
    // The writer schema in the commit metadata lacks the partition column when the writer drops
    // partition columns from the data files, as TableSchemaResolver.appendPartitionColumns handles.
    InternalTable table =
        describe(newMetaClient("dropped_partition_column", true, "level"), "level:VALUE");
    assertTrue(hasField(table, "level"), "the partition column must be part of the schema");
    assertEquals(1, table.getPartitioningFields().size());
    assertEquals("level", table.getPartitioningFields().get(0).getSourceField().getName());
  }

  private InternalTable describe(HoodieTableMetaClient metaClient, String partitionSpec) {
    HudiTableExtractor extractor =
        new HudiTableExtractor(
            new HudiSchemaExtractor(),
            HudiSourceConfig.fromPartitionFieldSpecConfig(partitionSpec)
                .loadSourcePartitionSpecExtractor());
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, WRITER_SCHEMA.toString());
    HoodieInstant commit =
        metaClient.createNewInstant(
            HoodieInstant.State.COMPLETED,
            HoodieTimeline.COMMIT_ACTION,
            "20260101000000000",
            "20260101000000001");
    return extractor.table(metaClient, commitMetadata, commit);
  }

  private HoodieTableMetaClient newMetaClient(
      String name, boolean populateMetaFields, String partitionField) throws Exception {
    HoodieTableMetaClient.TableBuilder builder =
        HoodieTableMetaClient.newTableBuilder()
            .setTableType(HoodieTableType.COPY_ON_WRITE)
            .setTableName(name)
            .setRecordKeyFields("key")
            .setPopulateMetaFields(populateMetaFields);
    if (partitionField != null) {
      builder.setPartitionFields(partitionField).setShouldDropPartitionColumns(true);
    }
    return builder.initTable(
        HadoopFSUtils.getStorageConf(new Configuration()),
        tempDir.resolve(name).toUri().toString());
  }

  private static boolean hasField(InternalTable table, String name) {
    Optional<InternalField> field =
        table.getReadSchema().getFields().stream()
            .filter(f -> name.equals(f.getName()))
            .findFirst();
    return field.isPresent();
  }
}
