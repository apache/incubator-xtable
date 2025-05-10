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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.ConvertedTable;

@ExtendWith(MockitoExtension.class)
class TestConversionResource {

  private static final String SOURCE_TABLE_NAME = "users";
  private static final String SOURCE_TABLE_BASE_PATH = "s3://bucket/tables/users";
  private static final String TARGET_ICEBERG_METADATA_PATH = "s3://bucket/tables/users/metadata";

  @Mock private ConversionService conversionService;

  @InjectMocks private ConversionResource resource;

  @Test
  void testConvertTableResource() {
    ConvertTableRequest req =
        ConvertTableRequest.builder()
            .sourceFormat(TableFormat.DELTA)
            .sourceTableName(SOURCE_TABLE_NAME)
            .sourceTablePath(SOURCE_TABLE_BASE_PATH)
            .targetFormats(Arrays.asList(TableFormat.ICEBERG))
            .build();

    ConvertedTable icebergTable =
        ConvertedTable.builder()
            .targetFormat(TableFormat.ICEBERG)
            .targetMetadataPath(TARGET_ICEBERG_METADATA_PATH)
            .build();

    ConvertTableResponse expected =
        ConvertTableResponse.builder().convertedTables(Arrays.asList(icebergTable)).build();
    when(conversionService.convertTable(req)).thenReturn(expected);
    ConvertTableResponse actual = resource.convertTable(req);
    verify(conversionService).convertTable(req);

    assertNotNull(actual);
    assertSame(expected, actual, "Resource should return the exact response from the service");

    assertEquals(1, actual.getConvertedTables().size());
    assertEquals(TableFormat.ICEBERG, actual.getConvertedTables().get(0).getTargetFormat());
    assertEquals(
        TARGET_ICEBERG_METADATA_PATH, actual.getConvertedTables().get(0).getTargetMetadataPath());
  }
}
