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
 
package org.apache.xtable.service.models;

import lombok.Builder;
import lombok.Getter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One progress record emitted while a conversion runs.
 *
 * <p>These are events the service records itself, not captured application log output. Quarkus
 * routes logging through the JBoss LogManager while {@code xtable-core} logs through Log4j2, so
 * capturing that output per run needs a logging bridge that this module does not currently declare.
 * Explicit events give the UI a progress trail without depending on that decision.
 *
 * <p>{@code sequence} is monotonic within a run and is what clients pass back as {@code after} to
 * fetch only what they have not seen.
 */
@Getter
@Builder
public class RunEvent {
  @JsonProperty("sequence")
  private long sequence;

  @JsonProperty("timestamp")
  private String timestamp;

  @JsonProperty("level")
  private String level;

  @JsonProperty("message")
  private String message;

  @JsonCreator
  public RunEvent(
      @JsonProperty("sequence") long sequence,
      @JsonProperty("timestamp") String timestamp,
      @JsonProperty("level") String level,
      @JsonProperty("message") String message) {
    this.sequence = sequence;
    this.timestamp = timestamp;
    this.level = level;
    this.message = message;
  }
}
