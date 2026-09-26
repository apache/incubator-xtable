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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.xtable.service.models.ConvertTableRequest;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Bounded, in-memory store of conversion runs, newest first.
 *
 * <p>History does not survive a restart and the oldest run is evicted once capacity is reached.
 * That is deliberate: durable history means choosing a datastore and owning a schema, which is a
 * larger decision than this feature needs. Callers should present the run list as a view of the
 * current process rather than as an audit log.
 */
@ApplicationScoped
public class ConversionRunStore {

  private final ConversionServiceConfig serviceConfig;
  private Map<String, ConversionRun> runs;

  @Inject
  public ConversionRunStore(ConversionServiceConfig serviceConfig) {
    this.serviceConfig = serviceConfig;
  }

  @PostConstruct
  void init() {
    final int capacity = serviceConfig.getRunHistoryCapacity();
    this.runs =
        Collections.synchronizedMap(
            new LinkedHashMap<String, ConversionRun>(16, 0.75f, false) {
              @Override
              protected boolean removeEldestEntry(Map.Entry<String, ConversionRun> eldest) {
                return size() > capacity;
              }
            });
  }

  /** Registers a new run in {@code RUNNING} state and returns it. */
  public ConversionRun create(ConvertTableRequest request) {
    ConversionRun run =
        new ConversionRun(UUID.randomUUID().toString(), request, serviceConfig.getRunMaxEvents());
    runs.put(run.getConversionId(), run);
    return run;
  }

  public Optional<ConversionRun> get(String conversionId) {
    return Optional.ofNullable(runs.get(conversionId));
  }

  /** All retained runs, newest first. */
  public List<ConversionRun> list() {
    List<ConversionRun> snapshot;
    synchronized (runs) {
      snapshot = new ArrayList<>(runs.values());
    }
    Collections.reverse(snapshot);
    return snapshot;
  }

  public int size() {
    return runs.size();
  }
}
