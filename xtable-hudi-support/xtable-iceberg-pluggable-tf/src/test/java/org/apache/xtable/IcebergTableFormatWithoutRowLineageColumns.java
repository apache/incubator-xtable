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
 
package org.apache.xtable;

import java.util.Collections;
import java.util.List;

import org.apache.hudi.common.FormatMetaColumn;

/**
 * The Iceberg table format as a Hudi writer without format meta column support sees it: the base
 * files it rewrites do not store the row lineage columns, while publishing to Iceberg is unchanged.
 */
public class IcebergTableFormatWithoutRowLineageColumns extends IcebergTableFormat {
  static final String NAME = "ICEBERG_WITHOUT_ROW_LINEAGE_COLUMNS";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<FormatMetaColumn> getFormatMetaColumns() {
    return Collections.emptyList();
  }
}
