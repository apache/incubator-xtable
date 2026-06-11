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
 * Specifies a set of privileges that can be granted or revoked for securable objects.
 *
 * <p>This enum is used to indicate the type of actions that a subject (user, role, group) is
 * allowed to perform on a given resource, such as a catalog, database, table, or other securable
 * entity.
 */
public enum InternalPrivilegeType {

  /** Grants all available privileges on the resource. */
  ALL,

  /**
   * Allows modification of the structure or metadata of the resource. For example, modifying
   * schemas or properties.
   */
  ALTER,

  /**
   * Allows describing or viewing the metadata of the resource. Typically applies to viewing schemas
   * or properties of the resource.
   */
  DESCRIBE,

  /**
   * Allows reading or selecting data from the resource. Commonly associated with performing SQL
   * SELECT statements.
   */
  SELECT,

  /**
   * Allows inserting new data into the resource. Typically granted for operations like SQL INSERT
   * statements.
   */
  INSERT,

  /**
   * Allows updating existing data within the resource. Typically granted for operations like SQL
   * UPDATE statements.
   */
  UPDATE,

  /** Allows creating new resources within the catalog. */
  CREATE,

  /** Allows deleting or dropping a resource, such as a database or a table. */
  DROP,

  /** Allows removing data from the resource, for example using SQL DELETE statements. */
  DELETE
}
