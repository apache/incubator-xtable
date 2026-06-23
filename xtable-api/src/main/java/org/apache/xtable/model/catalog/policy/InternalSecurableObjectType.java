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
 
package org.apache.xtable.model.catalog.policy;

/**
 * Identifies the type of securable object within the catalog.
 *
 * <p>Each attribute in this enum represents a different kind of resource for which permissions may
 * be managed or enforced.
 */
public enum InternalSecurableObjectType {

  /** Represents the root container in which databases and other objects reside. */
  CATALOG,

  /** Represents a logical grouping of tables and other related objects. */
  DATABASE,

  /** Represents a table, typically containing rows and columns of data. */
  TABLE,

  /** Represents a view, which is often a virtual table defined by a query. */
  VIEW,

  /**
   * Represents a partition, commonly used to segment table data for performance or organizational
   * reasons.
   */
  PARTITION,

  /** Represents a column, generally a single field within a table or partition. */
  COLUMN,

  /** Represents a function, such as a user-defined function (UDF) within the database system. */
  FUNCTION,

  /** Represents an unsupported object type for error handling. */
  UNSUPPORTED
}
