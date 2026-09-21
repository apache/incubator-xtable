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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.ConvertedTable;
import org.apache.xtable.service.models.RunEvent;
import org.apache.xtable.service.models.RunStatus;

@ExtendWith(MockitoExtension.class)
class TestAsyncConversionService {

  @Mock private ConversionService conversionService;
  @Mock private ConversionServiceConfig serviceConfig;

  private ExecutorService executor;
  private ConversionRunStore runStore;
  private AsyncConversionService asyncConversionService;

  @BeforeEach
  void setUp() {
    when(serviceConfig.getRunHistoryCapacity()).thenReturn(10);
    when(serviceConfig.getRunMaxEvents()).thenReturn(100);
    runStore = new ConversionRunStore(serviceConfig);
    runStore.init();
    // Single thread so the assertions below observe a deterministic ordering.
    executor = Executors.newSingleThreadExecutor();
    asyncConversionService =
        new AsyncConversionService(conversionService, runStore, serviceConfig, executor);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
  }

  private ConvertTableRequest request() {
    return ConvertTableRequest.builder()
        .sourceFormat("HUDI")
        .sourceTableName("people")
        .sourceTablePath("file:///tmp/hudi-dataset/people")
        .sourceDataPath("file:///tmp/hudi-dataset/people")
        .targetFormats(Arrays.asList("ICEBERG", "DELTA"))
        .build();
  }

  private void awaitCompletion() throws InterruptedException {
    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "conversion did not finish");
  }

  @Test
  void submitReturnsRunImmediatelyAndRecordsResult() throws InterruptedException {
    ConvertTableResponse response =
        ConvertTableResponse.builder()
            .convertedTables(
                Collections.singletonList(
                    ConvertedTable.builder()
                        .targetFormat("ICEBERG")
                        .targetMetadataPath("file:///tmp/hudi-dataset/people/metadata")
                        .targetSchema("{}")
                        .build()))
            .build();
    when(conversionService.convertTable(any())).thenReturn(response);

    ConversionRun run = asyncConversionService.submit(request());
    assertNotNull(run.getConversionId());
    assertTrue(runStore.get(run.getConversionId()).isPresent(), "run is pollable immediately");

    awaitCompletion();

    assertEquals(RunStatus.SUCCEEDED, run.getStatus());
    assertEquals(response, run.getResult());
    List<RunEvent> events = run.eventsAfter(0);
    assertTrue(
        events.stream().anyMatch(e -> e.getMessage().contains("Wrote ICEBERG metadata")),
        "per-target progress event is recorded");
    assertTrue(events.stream().anyMatch(e -> "Conversion succeeded".equals(e.getMessage())));
  }

  @Test
  void failureIsRecordedOnTheRunRatherThanThrown() throws InterruptedException {
    when(conversionService.convertTable(any()))
        .thenThrow(new IllegalArgumentException("source path not found"));

    ConversionRun run = asyncConversionService.submit(request());
    awaitCompletion();

    assertEquals(RunStatus.FAILED, run.getStatus());
    assertEquals("source path not found", run.toDetailView().getError());
    assertTrue(
        run.eventsAfter(0).stream().anyMatch(e -> "ERROR".equals(e.getLevel())),
        "an ERROR event is recorded");
  }

  /**
   * A missing engine on the classpath arrives as {@link java.util.ServiceConfigurationError}, which
   * is an {@link Error} rather than an {@link Exception}. If the worker only caught Exception the
   * run would never reach a terminal state and the UI would poll it forever.
   */
  @Test
  void errorNotJustExceptionMarksRunFailed() throws InterruptedException {
    when(conversionService.convertTable(any()))
        .thenThrow(new java.util.ServiceConfigurationError("io.delta.kernel.engine.Engine"));

    ConversionRun run = asyncConversionService.submit(request());
    awaitCompletion();

    assertEquals(RunStatus.FAILED, run.getStatus(), "an Error must still finish the run");
    assertEquals("io.delta.kernel.engine.Engine", run.toDetailView().getError());
  }

  @Test
  void exceptionWithoutMessageStillMarksRunFailed() throws InterruptedException {
    when(conversionService.convertTable(any())).thenThrow(new NullPointerException());

    ConversionRun run = asyncConversionService.submit(request());
    awaitCompletion();

    assertEquals(RunStatus.FAILED, run.getStatus());
    assertEquals(
        NullPointerException.class.getName(),
        run.toDetailView().getError(),
        "a message-less exception falls back to its class name");
  }
}
