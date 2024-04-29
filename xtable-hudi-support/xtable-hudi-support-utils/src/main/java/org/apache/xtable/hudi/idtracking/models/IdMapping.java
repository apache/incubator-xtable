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
 
package org.apache.xtable.hudi.idtracking.models;

import java.util.ArrayList;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Stores a mapping of field name to id and contains the same information for any children of this
 * field.
 */
@Getter
@EqualsAndHashCode
@ToString
public class IdMapping {
  private final String name;
  private final int id;

  @Setter private List<IdMapping> fields;

  public IdMapping(String name, int id) {
    this(name, id, new ArrayList<>());
  }

  @JsonCreator
  public IdMapping(
      @JsonProperty("name") String name,
      @JsonProperty("id") int id,
      @JsonProperty("fields") List<IdMapping> fields) {
    this.name = name;
    this.id = id;
    this.fields = fields;
  }
}
