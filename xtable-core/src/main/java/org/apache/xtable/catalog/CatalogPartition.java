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
 
package org.apache.xtable.catalog;

import java.util.List;

import lombok.Getter;

/**
 * This class is designed to encapsulate a set of partition values and the corresponding storage
 * location where the data for this partition is stored.
 */
@Getter
public class CatalogPartition {

  /**
   * A list of values defining this partition. For example, these values might correspond to
   * partition keys in a dataset (e.g., year, month, day).
   */
  private final List<String> values;

  /**
   * The storage location associated with this partition. Typically, this would be a path in a file
   * system or object store.
   */
  private final String storageLocation;

  public CatalogPartition(List<String> values, String storageLocation) {
    this.values = values;
    this.storageLocation = storageLocation;
  }
}
