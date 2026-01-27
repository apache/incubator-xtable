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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class TestHudiSourceConfig {

  @Test
  void testOmitMetadataFieldsFromProperties() {
    Properties properties = new Properties();
    properties.setProperty(HudiSourceConfig.OMIT_METADATA_FIELDS_CONFIG, "true");
    Configuration configuration = new Configuration(false);

    assertTrue(HudiSourceConfig.getOmitMetadataFields(properties, configuration));
  }

  @Test
  void testOmitMetadataFieldsFromHudiAlias() {
    Properties properties = new Properties();
    properties.setProperty(HudiSourceConfig.HUDI_OMIT_METADATA_FIELDS_CONFIG, "true");
    Configuration configuration = new Configuration(false);

    assertTrue(HudiSourceConfig.getOmitMetadataFields(properties, configuration));
  }

  @Test
  void testOmitMetadataFieldsFromConfiguration() {
    Properties properties = new Properties();
    Configuration configuration = new Configuration(false);
    configuration.set(HudiSourceConfig.OMIT_METADATA_FIELDS_CONFIG, "true");

    assertTrue(HudiSourceConfig.getOmitMetadataFields(properties, configuration));
  }

  @Test
  void testOmitMetadataFieldsDefaultsToFalse() {
    Properties properties = new Properties();
    Configuration configuration = new Configuration(false);

    assertFalse(HudiSourceConfig.getOmitMetadataFields(properties, configuration));
  }
}
