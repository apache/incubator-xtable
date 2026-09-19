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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.xtable.exception.NotSupportedException;

public class TestDeltaDeletionVectorHandler {
  private static final String DATA_FILE_PATH = "file:///table/part-0001.parquet";

  @Test
  public void rejectsDeletionVectorsByDefault() {
    DeltaDeletionVectorHandler handler = new DeltaDeletionVectorHandler(false);

    NotSupportedException exception =
        assertThrows(
            NotSupportedException.class, () -> handler.onDeletionVectorFound(DATA_FILE_PATH));

    assertTrue(exception.getMessage().contains(DATA_FILE_PATH));
    assertTrue(
        exception
            .getMessage()
            .contains(DeltaConversionSourceConfig.ALLOW_UNSUPPORTED_DELETION_VECTORS));
  }

  @Test
  public void warnsAndContinuesWhenExplicitlyAllowed() {
    List<String> warnings = new ArrayList<>();
    DeltaDeletionVectorHandler handler = new DeltaDeletionVectorHandler(true, warnings::add);

    handler.onDeletionVectorFound(DATA_FILE_PATH);

    assertEquals(1, warnings.size());
    assertTrue(warnings.get(0).contains(DATA_FILE_PATH));
    assertTrue(warnings.get(0).contains("may contain rows that were deleted"));
  }
}
