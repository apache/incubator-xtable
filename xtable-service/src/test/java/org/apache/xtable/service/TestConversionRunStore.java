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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.RunEvent;
import org.apache.xtable.service.models.RunStatus;

@ExtendWith(MockitoExtension.class)
class TestConversionRunStore {

  private static final int CAPACITY = 3;
  private static final int MAX_EVENTS = 4;

  @Mock private ConversionServiceConfig serviceConfig;

  private ConversionRunStore store;

  @BeforeEach
  void setUp() {
    when(serviceConfig.getRunHistoryCapacity()).thenReturn(CAPACITY);
    store = new ConversionRunStore(serviceConfig);
    store.init();
  }

  private ConvertTableRequest request(String tableName) {
    return ConvertTableRequest.builder()
        .sourceFormat("HUDI")
        .sourceTableName(tableName)
        .sourceTablePath("file:///tmp/hudi-dataset/" + tableName)
        .targetFormats(Arrays.asList("ICEBERG", "DELTA"))
        .build();
  }

  @Test
  void createRegistersRunAsRunning() {
    when(serviceConfig.getRunMaxEvents()).thenReturn(MAX_EVENTS);

    ConversionRun run = store.create(request("people"));

    assertEquals(RunStatus.RUNNING, run.getStatus());
    assertFalse(run.isFinished());
    assertTrue(store.get(run.getConversionId()).isPresent());
  }

  @Test
  void evictsOldestBeyondCapacity() {
    when(serviceConfig.getRunMaxEvents()).thenReturn(MAX_EVENTS);

    ConversionRun first = store.create(request("t1"));
    store.create(request("t2"));
    store.create(request("t3"));
    ConversionRun fourth = store.create(request("t4"));

    assertEquals(CAPACITY, store.size());
    assertFalse(store.get(first.getConversionId()).isPresent(), "oldest run should be evicted");
    assertTrue(store.get(fourth.getConversionId()).isPresent());
  }

  @Test
  void listsNewestFirst() {
    when(serviceConfig.getRunMaxEvents()).thenReturn(MAX_EVENTS);

    store.create(request("t1"));
    ConversionRun newest = store.create(request("t2"));

    List<ConversionRun> runs = store.list();

    assertEquals(2, runs.size());
    assertEquals(newest.getConversionId(), runs.get(0).getConversionId());
  }

  @Test
  void terminalTransitionsAreRecorded() {
    when(serviceConfig.getRunMaxEvents()).thenReturn(MAX_EVENTS);

    ConversionRun succeeded = store.create(request("ok"));
    succeeded.markSucceeded(null);
    ConversionRun failed = store.create(request("bad"));
    failed.markFailed("source path not found");

    assertEquals(RunStatus.SUCCEEDED, succeeded.getStatus());
    assertTrue(succeeded.isFinished());
    assertEquals(RunStatus.FAILED, failed.getStatus());
    assertEquals("source path not found", failed.toDetailView().getError());
  }

  @Test
  void eventsAfterReturnsOnlyNewerEventsAndBufferIsBounded() {
    when(serviceConfig.getRunMaxEvents()).thenReturn(MAX_EVENTS);
    ConversionRun run = store.create(request("people"));

    for (int i = 1; i <= MAX_EVENTS + 2; i++) {
      run.addEvent("INFO", "event " + i);
    }

    // Oldest two evicted, sequence numbers keep counting.
    assertEquals(2, run.getDroppedEvents());
    List<RunEvent> all = run.eventsAfter(0);
    assertEquals(MAX_EVENTS, all.size());
    assertEquals("event 3", all.get(0).getMessage());

    long lastSeen = all.get(all.size() - 1).getSequence();
    assertTrue(run.eventsAfter(lastSeen).isEmpty(), "no events newer than the last seen");

    run.addEvent("INFO", "another");
    List<RunEvent> delta = run.eventsAfter(lastSeen);
    assertEquals(1, delta.size());
    assertEquals("another", delta.get(0).getMessage());
  }
}
