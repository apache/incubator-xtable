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
 
package org.apache.xtable.utilities;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.Optional;

import lombok.SneakyThrows;

import org.junit.jupiter.api.Test;

import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.delta.DeltaConversionSourceConfig;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.SourceTableIdentifier;
import org.apache.xtable.utilities.RunCatalogSync.DatasetConfig.StorageIdentifier;

class TestRunCatalogSync {

  @SneakyThrows
  @Test
  void testMain() {
    String catalogConfigYamlPath =
        TestRunCatalogSync.class.getClassLoader().getResource("catalogConfig.yaml").getPath();
    String[] args = {"-catalogConfig", catalogConfigYamlPath};
    // Ensure yaml gets parsed without any errors.
    assertDoesNotThrow(() -> RunCatalogSync.main(args));
  }

  @Test
  void testAllowUnsupportedDeletionVectorsIsAppliedToSourceOnly() {
    SourceTableIdentifier sourceTableIdentifier =
        SourceTableIdentifier.builder()
            .storageIdentifier(
                StorageIdentifier.builder()
                    .tableFormat(TableFormat.DELTA)
                    .tableBasePath("file:///delta-table")
                    .tableName("delta_table")
                    .build())
            .allowUnsupportedDeletionVectors("true")
            .build();

    SourceTable sourceTable =
        RunCatalogSync.getSourceTable(sourceTableIdentifier, Optional.empty());

    assertEquals(
        "true",
        sourceTable
            .getAdditionalProperties()
            .getProperty(DeltaConversionSourceConfig.ALLOW_UNSUPPORTED_DELETION_VECTORS));
    assertNull(
        RunCatalogSync.getTargetProperties(sourceTable)
            .getProperty(DeltaConversionSourceConfig.ALLOW_UNSUPPORTED_DELETION_VECTORS));
  }

  @Test
  void testAllowUnsupportedDeletionVectorsUsesStrictBooleanParsing() throws IOException {
    String[][] configuredValues = {
      {"true", "true"},
      {"TRUE", "true"},
      {"false", "false"},
      {"yes", "false"},
      {"on", "false"},
      {"1", "false"}
    };

    for (String[] configuredValue : configuredValues) {
      RunCatalogSync.DatasetConfig config =
          RunCatalogSync.YAML_MAPPER.readValue(
              "datasets:\n"
                  + "  - sourceCatalogTableIdentifier:\n"
                  + "      allowUnsupportedDeletionVectors: "
                  + configuredValue[0],
              RunCatalogSync.DatasetConfig.class);

      assertEquals(
          configuredValue[1],
          config
              .getDatasets()
              .get(0)
              .getSourceCatalogTableIdentifier()
              .getAllowUnsupportedDeletionVectors());
    }
  }
}
