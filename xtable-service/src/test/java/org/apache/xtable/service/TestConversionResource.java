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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.service.models.ConversionRunView;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.ConvertedTable;
import org.apache.xtable.service.models.RunStatus;
import org.apache.xtable.service.models.SubmittedConversionResponse;

import jakarta.ws.rs.core.Response;

@ExtendWith(MockitoExtension.class)
class TestConversionResource {

  private static final String SOURCE_TABLE_NAME = "users";
  private static final String SOURCE_TABLE_BASE_PATH = "s3://bucket/tables/users";
  private static final String TARGET_ICEBERG_METADATA_PATH = "s3://bucket/tables/users/metadata";
  private static final String CONVERSION_ID = "6a3f0d4e-0000-4000-8000-000000000001";

  @Mock private ConversionService conversionService;
  @Mock private AsyncConversionService asyncConversionService;
  @Mock private ConversionRunStore runStore;

  @InjectMocks private ConversionResource resource;

  private ConvertTableRequest request() {
    return ConvertTableRequest.builder()
        .sourceFormat(TableFormat.DELTA)
        .sourceTableName(SOURCE_TABLE_NAME)
        .sourceTablePath(SOURCE_TABLE_BASE_PATH)
        .targetFormats(Arrays.asList(TableFormat.ICEBERG))
        .build();
  }

  private ConvertTableResponse response() {
    return ConvertTableResponse.builder()
        .convertedTables(
            Arrays.asList(
                ConvertedTable.builder()
                    .targetFormat(TableFormat.ICEBERG)
                    .targetMetadataPath(TARGET_ICEBERG_METADATA_PATH)
                    .build()))
        .build();
  }

  private ConversionRun run(ConvertTableRequest request) {
    return new ConversionRun(CONVERSION_ID, request, 100);
  }

  /** The released synchronous behaviour, which must not change when no Prefer header is sent. */
  @Test
  void testConvertTableResource() {
    ConvertTableRequest req = request();
    ConvertTableResponse expected = response();
    when(conversionService.convertTable(req)).thenReturn(expected);

    Response httpResponse = resource.convertTable(null, req);
    verify(conversionService).convertTable(req);
    verify(asyncConversionService, never()).submit(any());

    assertEquals(200, httpResponse.getStatus());
    ConvertTableResponse actual = (ConvertTableResponse) httpResponse.getEntity();
    assertNotNull(actual);
    assertSame(expected, actual, "Resource should return the exact response from the service");
    assertEquals(1, actual.getConvertedTables().size());
    assertEquals(TableFormat.ICEBERG, actual.getConvertedTables().get(0).getTargetFormat());
    assertEquals(
        TARGET_ICEBERG_METADATA_PATH, actual.getConvertedTables().get(0).getTargetMetadataPath());
  }

  @Test
  void preferRespondAsyncReturnsAcceptedWithConversionId() {
    ConvertTableRequest req = request();
    when(asyncConversionService.submit(req)).thenReturn(run(req));

    Response httpResponse = resource.convertTable("respond-async", req);

    assertEquals(202, httpResponse.getStatus());
    SubmittedConversionResponse body = (SubmittedConversionResponse) httpResponse.getEntity();
    assertEquals(CONVERSION_ID, body.getConversionId());
    verify(conversionService, never()).convertTable(any());
  }

  @Test
  void preferHeaderMatchIsCaseInsensitive() {
    ConvertTableRequest req = request();
    when(asyncConversionService.submit(req)).thenReturn(run(req));

    assertEquals(202, resource.convertTable("Respond-Async", req).getStatus());
  }

  @Test
  void statusIsAcceptedWhileRunningAndOkOnceFinished() {
    ConvertTableRequest req = request();
    ConversionRun conversionRun = run(req);
    when(runStore.get(CONVERSION_ID)).thenReturn(Optional.of(conversionRun));

    assertEquals(
        202,
        resource.getConversionStatus(CONVERSION_ID).getStatus(),
        "still running should poll as 202");

    conversionRun.markSucceeded(response());
    Response finished = resource.getConversionStatus(CONVERSION_ID);
    assertEquals(200, finished.getStatus());
    ConversionRunView view = (ConversionRunView) finished.getEntity();
    assertEquals(RunStatus.SUCCEEDED, view.getStatus());
    assertNotNull(view.getResult());
  }

  @Test
  void statusOfUnknownConversionIsNotFound() {
    when(runStore.get(CONVERSION_ID)).thenReturn(Optional.empty());

    assertEquals(404, resource.getConversionStatus(CONVERSION_ID).getStatus());
    assertEquals(404, resource.getRun(CONVERSION_ID).getStatus());
    assertEquals(404, resource.getRunEvents(CONVERSION_ID, 0).getStatus());
  }

  @Test
  void listRunsReturnsSummariesNewestFirst() {
    ConvertTableRequest req = request();
    when(runStore.list()).thenReturn(Arrays.asList(run(req), run(req)));

    List<ConversionRunView> runs = resource.listRuns();

    assertEquals(2, runs.size());
    assertEquals(TableFormat.DELTA, runs.get(0).getSourceFormat());
    assertEquals(SOURCE_TABLE_NAME, runs.get(0).getSourceTableName());
  }

  @Test
  void eventsAfterCursorReturnsOnlyNewEvents() {
    ConvertTableRequest req = request();
    ConversionRun conversionRun = run(req);
    conversionRun.addEvent("INFO", "first");
    conversionRun.addEvent("INFO", "second");
    when(runStore.get(CONVERSION_ID)).thenReturn(Optional.of(conversionRun));

    Response all = resource.getRunEvents(CONVERSION_ID, 0);
    assertEquals(200, all.getStatus());
    assertEquals(2, ((List<?>) all.getEntity()).size());

    Response afterFirst = resource.getRunEvents(CONVERSION_ID, 1);
    assertTrue(((List<?>) afterFirst.getEntity()).size() == 1);
  }
}
