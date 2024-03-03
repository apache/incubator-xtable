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
package io.onetable.constants;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class OneTableConstants {
  /**
   * Maximum number of syncs that are persisted in the archive file, after that least recent sync is
   * evicted.
   */
  public static final Integer NUM_ARCHIVED_SYNCS_RESULTS = 10;

  /** OneTable meta directory inside table base path to store sync info. */
  public static final String ONETABLE_META_DIR = ".onetable";
}
