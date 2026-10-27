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
 
package org.apache.xtable.spark.extensions

import org.apache.spark.sql.SparkSessionExtensions

import org.apache.xtable.spark.extensions.execution.XTableIndexStrategy
import org.apache.xtable.spark.extensions.optimizer.IndexPruningRule
import org.apache.xtable.spark.extensions.parser.XTableSqlParser

/**
 * Spark SQL extensions for XTable. Add them after Iceberg's extensions:
 *
 * {{{
 * spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,
 *   org.apache.xtable.spark.extensions.XTableSparkSessionExtensions
 * }}}
 *
 * They add the commands {@code CREATE INDEX ... USING xtable}, {@code DROP INDEX} and
 * {@code REFRESH INDEX} for Iceberg tables, and a rule that uses the index to read only the files
 * that hold the keys of an equality or {@code IN} filter.
 */
class XTableSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { case (_, delegate) => new XTableSqlParser(delegate) }
    extensions.injectPlannerStrategy(_ => XTableIndexStrategy)
    extensions.injectOptimizerRule(spark => IndexPruningRule(spark))
  }
}
