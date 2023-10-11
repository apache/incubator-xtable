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
 
package io.onetable.hudi;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Singleton;

import org.apache.avro.Schema;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import io.onetable.exception.SchemaExtractorException;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.DataLayoutStrategy;
import io.onetable.model.storage.TableFormat;
import io.onetable.schema.SchemaFieldFinder;
import io.onetable.spi.extractor.SourcePartitionSpecExtractor;

/** Extracts {@link OneTable} a canonical representation of table at a point in time for Hudi. */
@Singleton
public class HudiTableExtractor {
  private final HudiSchemaExtractor schemaExtractor;
  private final SourcePartitionSpecExtractor partitionSpecExtractor;

  public HudiTableExtractor(
      HudiSchemaExtractor schemaExtractor,
      SourcePartitionSpecExtractor sourcePartitionSpecExtractor) {
    this.schemaExtractor = schemaExtractor;
    this.partitionSpecExtractor = sourcePartitionSpecExtractor;
  }

  public OneTable table(HoodieTableMetaClient metaClient, HoodieInstant commit) {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    OneSchema canonicalSchema;
    Schema avroSchema;
    try {
      avroSchema = tableSchemaResolver.getTableAvroSchema(commit.getTimestamp());
      canonicalSchema = schemaExtractor.schema(avroSchema);
    } catch (Exception e) {
      throw new SchemaExtractorException(
          String.format(
              "Failed to convert table %s schema", metaClient.getTableConfig().getTableName()),
          e);
    }
    List<OnePartitionField> partitionFields = partitionSpecExtractor.spec(canonicalSchema);
    Set<OneField> recordKeyFields = getRecordKeyFields(metaClient, canonicalSchema);
    if (!recordKeyFields.isEmpty()) {
      canonicalSchema = canonicalSchema.toBuilder().recordKeyFields(recordKeyFields).build();
    }
    DataLayoutStrategy dataLayoutStrategy =
        partitionFields.size() > 0
            ? DataLayoutStrategy.DIR_HIERARCHY_PARTITION_VALUES
            : DataLayoutStrategy.FLAT;
    return OneTable.builder()
        .tableFormat(TableFormat.HUDI)
        .basePath(metaClient.getBasePathV2().toString())
        .name(metaClient.getTableConfig().getTableName())
        .layoutStrategy(dataLayoutStrategy)
        .partitioningFields(partitionFields)
        .readSchema(canonicalSchema)
        .latestCommitTime(HudiClient.parseFromInstantTime(commit.getTimestamp()))
        .build();
  }

  private Set<OneField> getRecordKeyFields(
      HoodieTableMetaClient metaClient, OneSchema canonicalSchema) {
    Option<String[]> recordKeyFieldNames = metaClient.getTableConfig().getRecordKeyFields();
    if (!recordKeyFieldNames.isPresent()) {
      return Collections.emptySet();
    }
    return Arrays.stream(recordKeyFieldNames.get())
        .map(name -> SchemaFieldFinder.getInstance().findFieldByPath(canonicalSchema, name))
        .collect(Collectors.toSet());
  }
}
