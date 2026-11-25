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
 
package org.apache.xtable.hudi.extensions;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.ParquetSchemaUtil;

class TestHoodieAvroWriteSupportWithFieldIds {

  /**
   * A file with field ids is read by field id only, so the row lineage columns get their reserved
   * ids next to the ids of the table columns.
   */
  @Test
  void rowLineageColumnsGetTheirReservedFieldIds() {
    MessageType fileSchema =
        Types.buildMessage()
            .required(BINARY)
            .named("key")
            .optional(INT64)
            .named(MetadataColumns.ROW_ID.name())
            .optional(INT64)
            .named(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())
            .named("table");
    List<MappedField> mappedFields = new ArrayList<>();
    mappedFields.add(MappedField.of(1, "key"));
    HoodieAvroWriteSupportWithFieldIds.addRowLineageFields(mappedFields);

    MessageType withIds =
        ParquetSchemaUtil.applyNameMapping(fileSchema, NameMapping.of(mappedFields));
    assertEquals(1, withIds.getType("key").getId().intValue());
    assertEquals(
        MetadataColumns.ROW_ID.fieldId(),
        withIds.getType(MetadataColumns.ROW_ID.name()).getId().intValue());
    assertEquals(
        MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
        withIds.getType(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()).getId().intValue());
  }

  @Test
  void aTableColumnWithTheNameOfARowLineageColumnKeepsItsId() {
    List<MappedField> mappedFields =
        new ArrayList<>(Arrays.asList(MappedField.of(7, MetadataColumns.ROW_ID.name())));
    HoodieAvroWriteSupportWithFieldIds.addRowLineageFields(mappedFields);
    assertEquals(
        Arrays.asList(
            MappedField.of(7, MetadataColumns.ROW_ID.name()),
            MappedField.of(
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())),
        mappedFields);
  }
}
