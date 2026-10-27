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
 
package org.apache.xtable.spark.extensions.execution

import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical.{CreateIndex, DropIndex, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

import org.apache.iceberg.spark.source.SparkTable

import org.apache.xtable.spark.extensions.IndexDefinition
import org.apache.xtable.spark.extensions.plans.RefreshIndex

/**
 * Plans the index commands for Iceberg tables. Iceberg's {@code SparkTable} does not implement
 * Spark's {@code SupportsIndex}, so without this strategy Spark rejects {@code CREATE INDEX} and
 * {@code DROP INDEX} on Iceberg tables. Index types other than {@code xtable} are left to Spark.
 */
object XTableIndexStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateIndex(
          ResolvedTable(catalog, ident, table: SparkTable, _),
          indexName,
          indexType,
          ignoreIfExists,
          columns,
          properties) if IndexDefinition.IndexType.equalsIgnoreCase(indexType) =>
      CreateIndexExec(
        catalog,
        ident,
        table,
        indexName,
        columns.map(_._1.name),
        ignoreIfExists,
        properties) :: Nil

    case DropIndex(
          ResolvedTable(catalog, ident, table: SparkTable, _),
          indexName,
          ignoreIfNotExists) =>
      DropIndexExec(catalog, ident, table, indexName, ignoreIfNotExists) :: Nil

    case RefreshIndex(ResolvedTable(catalog, ident, table: SparkTable, _), indexName) =>
      RefreshIndexExec(catalog, ident, table, indexName) :: Nil

    case RefreshIndex(ResolvedTable(_, ident, _, _), _) =>
      throw new UnsupportedOperationException(
        s"REFRESH INDEX is only supported for Iceberg tables, but $ident is not one")

    case _ => Nil
  }
}
