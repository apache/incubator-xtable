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

import java.util.List;

import lombok.Builder;
import lombok.Getter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Serialized view of a conversion run, used by the run list and the run detail screens. */
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConversionRunView {
  @JsonProperty("conversion-id")
  private String conversionId;

  @JsonProperty("status")
  private RunStatus status;

  @JsonProperty("source-format")
  private String sourceFormat;

  @JsonProperty("source-table-name")
  private String sourceTableName;

  @JsonProperty("source-table-path")
  private String sourceTablePath;

  @JsonProperty("target-formats")
  private List<String> targetFormats;

  @JsonProperty("started-at")
  private String startedAt;

  @JsonProperty("finished-at")
  private String finishedAt;

  @JsonProperty("duration-millis")
  private Long durationMillis;

  @JsonProperty("error")
  private String error;

  @JsonProperty("result")
  private ConvertTableResponse result;
}
