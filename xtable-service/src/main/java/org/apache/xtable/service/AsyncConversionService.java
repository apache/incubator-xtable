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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.log4j.Log4j2;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Runs conversions off the request thread so the caller can poll for progress.
 *
 * <p>This wraps {@link ConversionService#convertTable} rather than duplicating any conversion
 * logic. The synchronous endpoint keeps calling {@link ConversionService} directly.
 */
@Log4j2
@ApplicationScoped
public class AsyncConversionService {

  private final ConversionService conversionService;
  private final ConversionRunStore runStore;
  private final ConversionServiceConfig serviceConfig;
  private ExecutorService executor;

  @Inject
  public AsyncConversionService(
      ConversionService conversionService,
      ConversionRunStore runStore,
      ConversionServiceConfig serviceConfig) {
    this.conversionService = conversionService;
    this.runStore = runStore;
    this.serviceConfig = serviceConfig;
  }

  @PostConstruct
  void init() {
    this.executor =
        Executors.newFixedThreadPool(
            serviceConfig.getAsyncWorkerThreads(), namedDaemonThreadFactory());
  }

  @PreDestroy
  void shutdown() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @VisibleForTesting
  AsyncConversionService(
      ConversionService conversionService,
      ConversionRunStore runStore,
      ConversionServiceConfig serviceConfig,
      ExecutorService executor) {
    this.conversionService = conversionService;
    this.runStore = runStore;
    this.serviceConfig = serviceConfig;
    this.executor = executor;
  }

  /**
   * Registers a run and schedules the conversion.
   *
   * @return the run, already registered in the store and safe to poll immediately
   */
  public ConversionRun submit(ConvertTableRequest request) {
    ConversionRun run = runStore.create(request);
    run.addEvent(
        "INFO",
        String.format(
            "Accepted conversion of %s table '%s' to %s",
            request.getSourceFormat(), request.getSourceTableName(), request.getTargetFormats()));
    executor.execute(() -> execute(run, request));
    return run;
  }

  private void execute(ConversionRun run, ConvertTableRequest request) {
    run.addEvent("INFO", "Conversion started");
    try {
      ConvertTableResponse response = conversionService.convertTable(request);
      if (response.getConvertedTables() != null) {
        response
            .getConvertedTables()
            .forEach(
                converted ->
                    run.addEvent(
                        "INFO",
                        String.format(
                            "Wrote %s metadata to %s",
                            converted.getTargetFormat(), converted.getTargetMetadataPath())));
      }
      run.markSucceeded(response);
      run.addEvent("INFO", "Conversion succeeded");
    } catch (Throwable t) {
      // Throwable, not Exception, on purpose. This worker owns the whole lifecycle of the run:
      // if it dies without marking a terminal state the run is stuck at RUNNING forever and the
      // UI polls it indefinitely. A missing engine on the classpath surfaces as
      // ServiceConfigurationError or NoClassDefFoundError, both Errors, and those are exactly the
      // failures a user most needs to see reported.
      log.error("Conversion {} failed", run.getConversionId(), t);
      String message = t.getMessage() == null ? t.getClass().getName() : t.getMessage();
      run.addEvent("ERROR", message);
      run.markFailed(message);
    }
  }

  private static ThreadFactory namedDaemonThreadFactory() {
    AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      Thread thread = new Thread(runnable, "xtable-conversion-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }
}
