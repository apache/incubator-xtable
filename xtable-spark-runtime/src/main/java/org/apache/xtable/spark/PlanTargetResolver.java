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
 
package org.apache.xtable.spark;

import java.net.URI;
import java.util.Optional;

import lombok.extern.log4j.Log4j2;

import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;

/**
 * Best-effort extraction of the base path written by a query, from its analyzed {@link
 * LogicalPlan}.
 *
 * <p>Returns the written path only for recognized write commands; reads and unrecognized plans
 * yield an empty result and the caller skips them (so a read never triggers a sync). The fragile
 * dependence on Spark internal command types is deliberately isolated here.
 */
@Log4j2
public final class PlanTargetResolver {

  private PlanTargetResolver() {}

  /** Returns the output base path of a write command at the root of the analyzed plan, if known. */
  public static Optional<String> resolveWrittenPath(QueryExecution qe) {
    if (qe == null) {
      return Optional.empty();
    }
    try {
      LogicalPlan plan = qe.analyzed();
      if (plan instanceof InsertIntoHadoopFsRelationCommand) {
        return Optional.ofNullable(((InsertIntoHadoopFsRelationCommand) plan).outputPath())
            .map(Object::toString);
      }
      if (plan instanceof SaveIntoDataSourceCommand) {
        scala.Option<String> path = ((SaveIntoDataSourceCommand) plan).options().get("path");
        return path.isDefined() ? Optional.of(path.get()) : Optional.empty();
      }
      if (plan instanceof CreateDataSourceTableAsSelectCommand) {
        CatalogTable table = ((CreateDataSourceTableAsSelectCommand) plan).table();
        scala.Option<URI> location = table.storage().locationUri();
        return location.isDefined() ? Optional.of(location.get().toString()) : Optional.empty();
      }
    } catch (Throwable t) {
      // Best-effort only: any failure to introspect the plan falls back to the all-tables path.
      log.debug("Could not resolve written path from query plan; falling back to all tables", t);
    }
    return Optional.empty();
  }
}
