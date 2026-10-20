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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;

/**
 * Reproduction for issue #972: with delta.columnMapping.mode=name, the add actions in the Delta
 * log key partitionValues by the PHYSICAL column name (e.g. "col-<uuid>"), while the internal
 * partition field carries the LOGICAL name ("country"). Extraction must still resolve the value.
 */
public class TestDeltaKernelPartitionExtractorColumnMappingRepro {
  private static final String PHYSICAL_NAME = "col-1a2b3c4d-5e6f-4a5b-8c9d-0e1f2a3b4c5d";
  private final DeltaKernelPartitionExtractor extractor = DeltaKernelPartitionExtractor.getInstance();

  private static InternalPartitionField countryPartitionField() {
    return InternalPartitionField.builder()
        .sourceField(
            InternalField.builder()
                .name("country")
                .storageName(PHYSICAL_NAME)
                .schema(
                    InternalSchema.builder()
                        .name("string")
                        .dataType(InternalType.STRING)
                        .build())
                .build())
        .transformType(PartitionTransformType.VALUE)
        .build();
  }

  @Test
  public void testPartitionValueExtractionWithPhysicalNameKey() {
    // As written by Delta under column mapping: {"col-<uuid>": "USA"}
    Map<String, String> partitionValuesMap =
        new HashMap<>(Collections.singletonMap(PHYSICAL_NAME, "USA"));
    InternalPartitionField partitionField = countryPartitionField();
    List<PartitionValue> expected =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(partitionField)
                .range(Range.scalar("USA"))
                .build());
    List<PartitionValue> actual =
        extractor.partitionValueExtraction(partitionValuesMap, Arrays.asList(partitionField));
    assertEquals(expected, actual);
  }

  @Test
  public void testPartitionValueExtractionWithLogicalNameKeyStillWorks() {
    // Control: no column mapping, log keys the value by the logical name.
    Map<String, String> partitionValuesMap =
        new HashMap<>(Collections.singletonMap("country", "USA"));
    InternalPartitionField partitionField = countryPartitionField();
    List<PartitionValue> expected =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(partitionField)
                .range(Range.scalar("USA"))
                .build());
    List<PartitionValue> actual =
        extractor.partitionValueExtraction(partitionValuesMap, Arrays.asList(partitionField));
    assertEquals(expected, actual);
  }
}
