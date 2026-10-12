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

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.xtable.service.models.ConversionRunView;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.RunEvent;
import org.apache.xtable.service.models.RunStatus;

/**
 * Mutable record of one asynchronous conversion.
 *
 * <p>Written by the worker thread that runs the conversion and read by request threads serving the
 * status, run list and event endpoints, so every accessor is synchronized on the instance. The
 * event list is capped: a conversion of a large table can emit many events and an unbounded list
 * behind a UI is a memory leak.
 */
public class ConversionRun {

  private final String conversionId;
  private final ConvertTableRequest request;
  private final Instant startedAt;
  private final int maxEvents;
  private final List<RunEvent> events = new ArrayList<>();

  private RunStatus status = RunStatus.RUNNING;
  private Instant finishedAt;
  private ConvertTableResponse result;
  private String error;
  private long nextSequence = 1L;
  private long droppedEvents = 0L;

  public ConversionRun(String conversionId, ConvertTableRequest request, int maxEvents) {
    this.conversionId = conversionId;
    this.request = request;
    this.startedAt = Instant.now();
    this.maxEvents = maxEvents;
  }

  public String getConversionId() {
    return conversionId;
  }

  public synchronized RunStatus getStatus() {
    return status;
  }

  public synchronized ConvertTableResponse getResult() {
    return result;
  }

  /** Records a progress event, evicting the oldest once {@link #maxEvents} is reached. */
  public synchronized void addEvent(String level, String message) {
    events.add(
        RunEvent.builder()
            .sequence(nextSequence++)
            .timestamp(DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
            .level(level)
            .message(message)
            .build());
    while (events.size() > maxEvents) {
      events.remove(0);
      droppedEvents++;
    }
  }

  /** Returns events with a sequence strictly greater than {@code after}, oldest first. */
  public synchronized List<RunEvent> eventsAfter(long after) {
    List<RunEvent> selected = new ArrayList<>();
    for (RunEvent event : events) {
      if (event.getSequence() > after) {
        selected.add(event);
      }
    }
    return Collections.unmodifiableList(selected);
  }

  public synchronized long getDroppedEvents() {
    return droppedEvents;
  }

  public synchronized void markSucceeded(ConvertTableResponse response) {
    this.result = response;
    this.status = RunStatus.SUCCEEDED;
    this.finishedAt = Instant.now();
  }

  public synchronized void markFailed(String errorMessage) {
    this.error = errorMessage;
    this.status = RunStatus.FAILED;
    this.finishedAt = Instant.now();
  }

  public synchronized boolean isFinished() {
    return status != RunStatus.RUNNING;
  }

  /** Summary view without the conversion result, for the run list. */
  public synchronized ConversionRunView toSummaryView() {
    return baseView().build();
  }

  /** Full view including the conversion result, for the run detail screen. */
  public synchronized ConversionRunView toDetailView() {
    return baseView().result(result).build();
  }

  private ConversionRunView.ConversionRunViewBuilder baseView() {
    return ConversionRunView.builder()
        .conversionId(conversionId)
        .status(status)
        .sourceFormat(request.getSourceFormat())
        .sourceTableName(request.getSourceTableName())
        .sourceTablePath(request.getSourceTablePath())
        .targetFormats(request.getTargetFormats())
        .startedAt(DateTimeFormatter.ISO_INSTANT.format(startedAt))
        .finishedAt(finishedAt == null ? null : DateTimeFormatter.ISO_INSTANT.format(finishedAt))
        .durationMillis(
            finishedAt == null ? null : Duration.between(startedAt, finishedAt).toMillis())
        .error(error);
  }
}
