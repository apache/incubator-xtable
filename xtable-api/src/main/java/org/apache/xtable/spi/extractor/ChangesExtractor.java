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
 
package org.apache.xtable.spi.extractor;

import org.apache.xtable.model.IncrementalTableChanges;

/**
 * Use the source {@link CLIENT} to extract the canonical representation of delta changes since the
 * specified {@link INSTANT}
 *
 * @param <CLIENT> Table client to extract changes.
 * @param <INSTANT> Instant from which changes needs to be extracted.
 */
public interface ChangesExtractor<CLIENT, INSTANT> {
  IncrementalTableChanges changes(CLIENT client, INSTANT sinceSnapshotInstant);
}
