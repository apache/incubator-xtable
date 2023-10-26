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
 
package io.onetable.model.schema;

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
public class OnePartitionField {
  // Source field the partition is based on
  OneField sourceField;
  // Ordered partition field names of the table. These are present when the transform type is not
  // VALUE. If it is not present, use the source field name directly.
  @Builder.Default List<String> partitionFieldNames = Collections.emptyList();
  // An enum describing how the source data was transformed into the partition value
  PartitionTransformType transformType;
}
