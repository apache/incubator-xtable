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
 
package org.apache.xtable.model.schema;

import java.util.Collections;
import java.util.List;

import lombok.Builder;
import lombok.Value;

/**
 * Represents logical information about a field used for partitioning.
 *
 * @since 0.1
 */
@Value
@Builder
public class InternalPartitionField {
  // Source field the partition is based on
  InternalField sourceField;
  /*
   * Ordered partition field names of the table if they differ from the source field name. Use the
   * source field name directly when this list is empty. These are present when the transform is
   * not `VALUE` and the source table format represents these as a separate field, like a generated
   * column in Delta Lake.
   * For example if there is dateOfBirth column in timestamp format and table is partitioned by
   * yearOfBirth and monthOfBirth columns which are generated columns and computed as
   * year(dateOfBirth) and month(dateOfBirth) respectively, then partitionFieldNames will be
   * ["yearOfBirth", "monthOfBirth"] and sourceField will be "dateOfBirth".
   */
  @Builder.Default List<String> partitionFieldNames = Collections.emptyList();
  // An enum describing how the source data was transformed into the partition value
  PartitionTransformType transformType;
}
