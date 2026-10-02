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
 
package org.apache.xtable.delta;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Field metadata keys that Delta Lake writes into a table's schema, and the names Delta uses for
 * the children of a collection within them. The Spark based and the Kernel based readers both read
 * the same keys, so they share these definitions.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {

  /** The column mapping id Delta assigns to a struct field, which is also its parquet field id. */
  public static final String DELTA_COLUMN_MAPPING_ID = "delta.columnMapping.id";

  /** The physical name a struct field takes in the parquet file under column mapping. */
  public static final String DELTA_COLUMN_MAPPING_NAME = "delta.columnMapping.physicalName";

  /**
   * Written by Delta 3.x when IcebergCompatV2 is enabled. Holds the parquet field ids of a field's
   * map keys, map values and list elements, keyed by the path those take in the file, for example
   * "col-1234.key". Delta assigns column mapping ids to struct fields only, so without these the
   * nested fields of a collection have no id to match a reader against.
   */
  public static final String DELTA_COLUMN_MAPPING_NESTED_IDS = "delta.columnMapping.nested.ids";

  /** The expression that generates the values of a Delta generated column. */
  public static final String DELTA_GENERATION_EXPRESSION = "delta.generationExpression";

  /** The name parquet gives a list element, as used in {@link #DELTA_COLUMN_MAPPING_NESTED_IDS}. */
  public static final String PARQUET_LIST_ELEMENT_FIELD_NAME = "element";

  /** The name parquet gives a map key, as used in {@link #DELTA_COLUMN_MAPPING_NESTED_IDS}. */
  public static final String PARQUET_MAP_KEY_FIELD_NAME = "key";

  /** The name parquet gives a map value, as used in {@link #DELTA_COLUMN_MAPPING_NESTED_IDS}. */
  public static final String PARQUET_MAP_VALUE_FIELD_NAME = "value";
}
