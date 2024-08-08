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
 
package org.apache.xtable.conversion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.xtable.model.sync.SyncMode;

class TestConversionConfig {

  @Test
  void defaultValueSet() {
    ConversionConfig conversionConfig =
        ConversionConfig.builder()
            .sourceTable(mock(SourceTable.class))
            .targetTables(Collections.singletonList(mock(TargetTable.class)))
            .build();

    assertEquals(SyncMode.INCREMENTAL, conversionConfig.getSyncMode());
  }

  @Test
  void errorIfSourceTableNotSet() {
    assertThrows(
        NullPointerException.class,
        () ->
            ConversionConfig.builder()
                .targetTables(Collections.singletonList(mock(TargetTable.class)))
                .build());
  }

  @Test
  void errorIfNoTargetsSet() {
    Exception thrownException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ConversionConfig.builder()
                    .sourceTable(mock(SourceTable.class))
                    .targetTables(Collections.emptyList())
                    .build());
    assertEquals("Please provide at-least one format to sync", thrownException.getMessage());
  }
}
