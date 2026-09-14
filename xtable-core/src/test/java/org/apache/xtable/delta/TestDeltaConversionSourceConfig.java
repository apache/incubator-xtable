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

import static org.apache.xtable.delta.DeltaConversionSourceConfig.ALLOW_UNSUPPORTED_DELETION_VECTORS;
import static org.apache.xtable.delta.DeltaConversionSourceConfig.REUSE_METADATA_ACROSS_COMMITS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestDeltaConversionSourceConfig {

  @Test
  public void nullPropertiesUsesDefault() {
    DeltaConversionSourceConfig config = DeltaConversionSourceConfig.fromProperties(null);
    assertFalse(config.isReuseMetadataAcrossCommits());
    assertFalse(config.isAllowUnsupportedDeletionVectors());
  }

  @Test
  public void absentKeyUsesDefault() {
    DeltaConversionSourceConfig config =
        DeltaConversionSourceConfig.fromProperties(new Properties());
    assertFalse(config.isReuseMetadataAcrossCommits());
    assertFalse(config.isAllowUnsupportedDeletionVectors());
  }

  @Test
  public void oneArgumentConstructorPreservesDefaults() {
    DeltaConversionSourceConfig config = new DeltaConversionSourceConfig(true);

    assertTrue(config.isReuseMetadataAcrossCommits());
    assertFalse(config.isAllowUnsupportedDeletionVectors());
  }

  @ParameterizedTest
  @CsvSource({
    "true, true",
    "TRUE, true",
    "True, true",
    "false, false",
    "FALSE, false",
    // Boolean.parseBoolean treats anything that is not "true" as false
    "yes, false"
  })
  public void reuseMetadataAcrossCommitsIsParsedFromProperties(String value, boolean expected) {
    Properties properties = new Properties();
    properties.setProperty(REUSE_METADATA_ACROSS_COMMITS, value);
    assertEquals(
        expected,
        DeltaConversionSourceConfig.fromProperties(properties).isReuseMetadataAcrossCommits());
  }

  @ParameterizedTest
  @CsvSource({
    "true, true",
    "TRUE, true",
    "True, true",
    "false, false",
    "FALSE, false",
    "yes, false"
  })
  public void allowUnsupportedDeletionVectorsIsParsedFromProperties(
      String value, boolean expected) {
    Properties properties = new Properties();
    properties.setProperty(ALLOW_UNSUPPORTED_DELETION_VECTORS, value);
    assertEquals(
        expected,
        DeltaConversionSourceConfig.fromProperties(properties).isAllowUnsupportedDeletionVectors());
  }
}
