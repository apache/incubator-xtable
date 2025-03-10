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
 
package org.apache.xtable.model;

import lombok.Builder;
import lombok.Value;

import org.apache.xtable.model.storage.DataFilesDiff;

/**
 * Captures the changes in a single commit/instant from the source table.
 *
 * @since 0.1
 */
@Value
@Builder(toBuilder = true)
public class TableChange {
  // Change in files at the specified instant
  DataFilesDiff filesDiff;

  /** The {@link InternalTable} at the commit time to which this table change belongs. */
  InternalTable tableAsOfChange;
}
