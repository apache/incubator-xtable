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

  // size of the deletion vector
  int size;

  // count of records deleted by this deletion vector
  long countRecordsDeleted;

  // physical path of the deletion vector file (absolute with scheme)
  String sourceDeletionVectorFilePath;

  // offset of deletion vector start in a deletion vector file
  int offset;

  /**
   * binary representation of the deletion vector. The consumer can use the {@link
   * #ordinalsIterator()} to extract the ordinals represented in the binary format.
   */
  byte[] binaryRepresentation;

  /**
   * Supplier for an iterator that returns the ordinals of records deleted by this deletion vector
   * in the linked data file, identified by {@link #dataFilePath}.
   *
   * <p>The {@link InternalDeletionVector} instance does not guarantee that a new or distinct result
   * will be returned each time the supplier is invoked. However, the supplier is expected to return
   * a new iterator for each call.
   */
  @Getter(AccessLevel.NONE)
  Supplier<Iterator<Long>> ordinalsSupplier;

  /**
   * @return An iterator that returns the ordinals of records deleted by this deletion vector in the
   *     linked data file. There is no guarantee that a new or distinct iterator will be returned
   *     each time the iterator is invoked.
   */
  public Iterator<Long> ordinalsIterator() {
    return ordinalsSupplier.get();
  }
}
