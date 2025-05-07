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
 
package org.apache.xtable.service;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.ConvertedTable;
import org.apache.xtable.service.spark.SparkHolder;
import org.apache.xtable.service.utils.ConversionServiceUtil;

@ExtendWith(MockitoExtension.class)
class TestConversionService {

  private static final String SOURCE_TABLE_NAME = "users";
  private static final String SOURCE_TABLE_BASE_PATH = "s3://bucket/tables/users";
  private static final String TARGET_ICEBERG_METADATA_PATH =
      "s3://bucket/tables/users/metadata/v1.metadata.json";
  private static final String TARGET_SCHEMA = "{\"schema\":[]}";
  private final Configuration conf = new Configuration();

  @Mock SparkHolder sparkHolder;
  @Mock ConversionServiceUtil conversionServiceUtil;
  @Mock ConversionController controller;
  @Mock JavaSparkContext jsc;

  @InjectMocks ConversionService service;

  @Test
  void serviceConvertTableWithDeltaSourceAndTargetIceberg() {
    when(sparkHolder.jsc()).thenReturn(jsc);
    when(jsc.hadoopConfiguration()).thenReturn(conf);
    when(conversionServiceUtil.getIcebergSchemaAndMetadataPath(SOURCE_TABLE_BASE_PATH, conf))
        .thenReturn(Pair.of(TARGET_ICEBERG_METADATA_PATH, TARGET_SCHEMA));

    ConvertTableRequest req =
        ConvertTableRequest.builder()
            .sourceFormat("DELTA")
            .sourceTableName(SOURCE_TABLE_NAME)
            .sourceTablePath(SOURCE_TABLE_BASE_PATH)
            .targetFormats(Collections.singletonList("ICEBERG"))
            .build();

    ConvertTableResponse resp = service.convertTable(req);
    verify(controller, times(1)).sync(any(ConversionConfig.class), any());

    ConvertedTable restTargetTable = resp.getConvertedTables().get(0);
    assertEquals("ICEBERG", restTargetTable.getTargetFormat());
    assertEquals(TARGET_ICEBERG_METADATA_PATH, restTargetTable.getTargetMetadataPath());
    assertEquals(TARGET_SCHEMA, restTargetTable.getTargetSchema());
  }

  @Test
  void serviceConvertTableWithInvalidFormats() {
    ConvertTableRequest invalidRequest =
        ConvertTableRequest.builder()
            .sourceFormat("notRealSoureFormat")
            .sourceTableName(SOURCE_TABLE_NAME)
            .sourceTablePath(SOURCE_TABLE_BASE_PATH)
            .targetFormats(Collections.singletonList("notRealTargetFormat"))
            .build();
    assertThrows(IllegalArgumentException.class, () -> service.convertTable(invalidRequest));
  }
}
