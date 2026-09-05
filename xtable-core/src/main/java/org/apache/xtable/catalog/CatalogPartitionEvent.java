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
 
package org.apache.xtable.catalog;

/** Partition Event captures any partition that needs to be added or updated. */
public class CatalogPartitionEvent {

  public enum PartitionEventType {
    ADD,
    UPDATE,
    DROP
  }

  public PartitionEventType eventType;
  public String storagePartition;

  CatalogPartitionEvent(PartitionEventType eventType, String storagePartition) {
    this.eventType = eventType;
    this.storagePartition = storagePartition;
  }

  public static CatalogPartitionEvent newPartitionAddEvent(String storagePartition) {
    return new CatalogPartitionEvent(PartitionEventType.ADD, storagePartition);
  }

  public static CatalogPartitionEvent newPartitionUpdateEvent(String storagePartition) {
    return new CatalogPartitionEvent(PartitionEventType.UPDATE, storagePartition);
  }

  public static CatalogPartitionEvent newPartitionDropEvent(String storagePartition) {
    return new CatalogPartitionEvent(PartitionEventType.DROP, storagePartition);
  }
}
