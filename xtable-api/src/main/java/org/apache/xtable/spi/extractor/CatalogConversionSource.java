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

import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;

/**
 * A client for converting the table with tableIdentifier {@link HierarchicalTableIdentifier} in
 * source catalog to SourceTable object {@link SourceTable}, can be used by downstream consumers for
 * syncing it to multiple {@link org.apache.xtable.conversion.TargetTable}
 */
public interface CatalogConversionSource {
  /** Returns the source table object present in the catalog. */
  SourceTable getSourceTable(HierarchicalTableIdentifier tableIdentifier);
}
