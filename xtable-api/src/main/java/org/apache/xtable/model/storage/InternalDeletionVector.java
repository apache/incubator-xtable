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
 
package org.apache.xtable.model.storage;

import java.util.Iterator;
import java.util.function.Supplier;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Builder(toBuilder = true, builderClassName = "Builder")
@Accessors(fluent = true)
@Value
public class InternalDeletionVector {
  // path (absolute with scheme) of data file to which this deletion vector belongs
  @NonNull String dataFilePath;

  // physical path of the deletion vector file (absolute with scheme)
  String deletionVectorFilePath;

  // offset of deletion vector start in the deletion vector file
  int offset;

  // length of the deletion vector in the deletion vector file
  int length;

  // count of records deleted by this deletion vector
  long countRecordsDeleted;

  @Getter(AccessLevel.NONE)
  Supplier<Iterator<Long>> deleteRecordSupplier;

  public Iterator<Long> deleteRecordIterator() {
    return deleteRecordSupplier.get();
  }

  public static class Builder {
    public Builder deleteRecordSupplier(Supplier<Iterator<Long>> recordsSupplier) {
      this.deleteRecordSupplier = recordsSupplier;
      return this;
    }
  }
}
