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
 * Defines a structure for obtaining a unique, canonical identifier for a securable object within
 * the catalog.
 *
 * <p>Implementations of this interface may represent entities such as catalogs, databases, tables,
 * or any other resource that can be protected or controlled via security policies.
 */
public interface InternalSecurableObjectIdentifier {

  /**
   * Returns the unique identifier of the securable object in a canonical form.
   *
   * @return a non-null {@link String} representing the unique identifier of this securable object
   */
  String getId();
}
