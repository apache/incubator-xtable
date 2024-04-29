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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTransform;

public class TestIcebergPartitionSpecSync {
  private Transaction mockTransaction;
  private UpdatePartitionSpec mockUpdatePartitionSpec;

  @BeforeEach
  public void setup() {
    mockTransaction = mock(Transaction.class);
    mockUpdatePartitionSpec = mock(UpdatePartitionSpec.class);
    when(mockTransaction.updateSpec()).thenReturn(mockUpdatePartitionSpec);
    Table mockTable = mock(Table.class);
    when(mockTransaction.table()).thenReturn(mockTable);
    when(mockTable.schema()).thenReturn(IcebergTestUtils.SCHEMA);
  }

  @Test
  public void testAddPartitionField() {
    PartitionSpec currentPartitionSpec = PartitionSpec.unpartitioned();
    PartitionSpec latestPartitionSpec =
        PartitionSpec.builderFor(IcebergTestUtils.SCHEMA).day("timestamp_field").build();

    IcebergPartitionSpecSync.getInstance()
        .sync(currentPartitionSpec, latestPartitionSpec, mockTransaction);

    ArgumentCaptor<Term> addedTerm = ArgumentCaptor.forClass(Term.class);
    verify(mockUpdatePartitionSpec).addField(addedTerm.capture());
    verify(mockUpdatePartitionSpec).commit();
    verify(mockUpdatePartitionSpec, never()).removeField(anyString());
    verify(mockUpdatePartitionSpec, never()).removeField(any(Term.class));

    UnboundTransform unboundTerm = (UnboundTransform) addedTerm.getValue();
    Assertions.assertEquals("timestamp_field", unboundTerm.ref().name());
    Assertions.assertEquals("day", unboundTerm.transform().toString());
  }

  @Test
  public void testRemovePartitionField() {
    PartitionSpec currentPartitionSpec =
        PartitionSpec.builderFor(IcebergTestUtils.SCHEMA)
            .day("timestamp_field")
            .identity("group_id")
            .build();
    PartitionSpec latestPartitionSpec =
        PartitionSpec.builderFor(IcebergTestUtils.SCHEMA).identity("group_id").build();

    IcebergPartitionSpecSync.getInstance()
        .sync(currentPartitionSpec, latestPartitionSpec, mockTransaction);

    ArgumentCaptor<String> removedFieldName = ArgumentCaptor.forClass(String.class);
    verify(mockUpdatePartitionSpec).removeField(removedFieldName.capture());
    verify(mockUpdatePartitionSpec).commit();
    verify(mockUpdatePartitionSpec, never()).addField(anyString());
    verify(mockUpdatePartitionSpec, never()).addField(any(Term.class));

    Assertions.assertEquals(1, removedFieldName.getAllValues().size());
    Assertions.assertEquals("timestamp_field_day", removedFieldName.getValue());
  }

  @Test
  public void testUpdateExistingPartitionField() {
    PartitionSpec currentPartitionSpec =
        PartitionSpec.builderFor(IcebergTestUtils.SCHEMA).day("timestamp_field").build();
    PartitionSpec latestPartitionSpec =
        PartitionSpec.builderFor(IcebergTestUtils.SCHEMA).month("timestamp_field").build();

    IcebergPartitionSpecSync.getInstance()
        .sync(currentPartitionSpec, latestPartitionSpec, mockTransaction);

    ArgumentCaptor<String> removedFieldName = ArgumentCaptor.forClass(String.class);
    verify(mockUpdatePartitionSpec).removeField(removedFieldName.capture());
    ArgumentCaptor<Term> addedTerm = ArgumentCaptor.forClass(Term.class);
    verify(mockUpdatePartitionSpec).addField(addedTerm.capture());
    verify(mockUpdatePartitionSpec).commit();

    Assertions.assertEquals("timestamp_field_day", removedFieldName.getValue());
    UnboundTransform unboundTerm = (UnboundTransform) addedTerm.getValue();
    Assertions.assertEquals("timestamp_field", unboundTerm.ref().name());
    Assertions.assertEquals("month", unboundTerm.transform().toString());
  }

  @Test
  public void testNoUpdate() {
    PartitionSpec currentPartitionSpec =
        PartitionSpec.builderFor(IcebergTestUtils.SCHEMA).day("timestamp_field").build();
    PartitionSpec latestPartitionSpec =
        PartitionSpec.builderFor(IcebergTestUtils.SCHEMA).day("timestamp_field").build();

    IcebergPartitionSpecSync.getInstance()
        .sync(currentPartitionSpec, latestPartitionSpec, mockTransaction);
    verify(mockUpdatePartitionSpec, never()).commit();
  }
}
