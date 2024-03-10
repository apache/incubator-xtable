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
 
package org.apache.xtable.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public class IcebergTestUtils {
  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone()),
          Types.NestedField.required(2, "date_field", Types.DateType.get()),
          Types.NestedField.required(3, "group_id", Types.IntegerType.get()),
          Types.NestedField.required(
              4,
              "record",
              Types.StructType.of(
                  Types.NestedField.required(5, "string_field", Types.StringType.get()))),
          Types.NestedField.required(
              6,
              "map_field",
              Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.IntegerType.get())),
          Types.NestedField.required(
              9, "array_field", Types.ListType.ofRequired(10, Types.IntegerType.get())));
}
