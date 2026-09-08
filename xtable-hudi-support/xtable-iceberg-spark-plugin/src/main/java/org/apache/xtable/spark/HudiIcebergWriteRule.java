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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression;
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation$;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Function1;
import scala.Option;
import scala.runtime.AbstractPartialFunction;

/**
 * Post-hoc resolution rule: a resolved write plan whose target is a {@link HudiSparkTable} becomes
 * an {@code InsertIntoDataSourceCommand} over a {@link HudiIcebergRelation}. It runs once, after
 * analysis and before Spark rewrites row-level commands, and leaves every plan it does not
 * understand untouched so the write falls back to Iceberg's own writer.
 */
public class HudiIcebergWriteRule extends Rule<LogicalPlan> {
  private static final Logger LOG = LoggerFactory.getLogger(HudiIcebergWriteRule.class);

  private final SparkSession spark;

  public HudiIcebergWriteRule(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public LogicalPlan apply(LogicalPlan plan) {
    if (!isEnabled()) {
      return plan;
    }
    return plan.resolveOperatorsUp(
        new AbstractPartialFunction<LogicalPlan, LogicalPlan>() {
          @Override
          public boolean isDefinedAt(LogicalPlan node) {
            return rewrite(node) != null;
          }

          @Override
          @SuppressWarnings("unchecked")
          public <A1 extends LogicalPlan, B1> B1 applyOrElse(A1 node, Function1<A1, B1> orElse) {
            LogicalPlan rewritten = rewrite(node);
            return rewritten != null ? (B1) rewritten : orElse.apply(node);
          }
        });
  }

  private boolean isEnabled() {
    return spark.conf().get(HudiIcebergConf.ENABLED, "true").equalsIgnoreCase("true");
  }

  private LogicalPlan rewrite(LogicalPlan node) {
    if (node instanceof AppendData) {
      AppendData append = (AppendData) node;
      DataSourceV2Relation target = managedTarget(append.table());
      if (target == null || !append.query().resolved()) {
        return null;
      }
      return command(target, append.query(), HudiWriteOperation.APPEND);
    }
    if (node instanceof OverwritePartitionsDynamic) {
      OverwritePartitionsDynamic overwrite = (OverwritePartitionsDynamic) node;
      DataSourceV2Relation target = managedTarget(overwrite.table());
      if (target == null || !overwrite.query().resolved()) {
        return null;
      }
      return command(target, overwrite.query(), HudiWriteOperation.INSERT_OVERWRITE);
    }
    if (node instanceof OverwriteByExpression) {
      OverwriteByExpression overwrite = (OverwriteByExpression) node;
      DataSourceV2Relation target = managedTarget(overwrite.table());
      if (target == null || !overwrite.query().resolved()) {
        return null;
      }
      HudiSparkTable table = (HudiSparkTable) target.table();
      if (overwrite.deleteExpr().equals(Literal.TrueLiteral())) {
        return command(target, overwrite.query(), HudiWriteOperation.INSERT_OVERWRITE_TABLE);
      }
      if (isStaticPartitionPredicate(overwrite.deleteExpr(), table)) {
        return command(target, overwrite.query(), HudiWriteOperation.INSERT_OVERWRITE);
      }
      LOG.warn(
          "Overwrite of {} with predicate {} stays on the Iceberg writer: only whole-table and "
              + "static partition overwrites are routed through Hudi",
          table.name(),
          overwrite.deleteExpr());
      return null;
    }
    return null;
  }

  private LogicalPlan command(
      DataSourceV2Relation target, LogicalPlan query, HudiWriteOperation operation) {
    HudiSparkTable table = (HudiSparkTable) target.table();
    TableCatalog catalog = null;
    Option<CatalogPlugin> plugin = target.catalog();
    if (plugin.isDefined() && plugin.get() instanceof TableCatalog) {
      catalog = (TableCatalog) plugin.get();
    }
    Identifier identifier = target.identifier().isDefined() ? target.identifier().get() : null;
    HudiIcebergRelation relation =
        new HudiIcebergRelation(spark, table, operation, catalog, identifier);
    LogicalRelation logicalRelation = LogicalRelation$.MODULE$.apply(relation, false);
    return new InsertIntoDataSourceCommand(
        logicalRelation, query, operation != HudiWriteOperation.APPEND);
  }

  private static DataSourceV2Relation managedTarget(NamedRelation table) {
    if (table instanceof DataSourceV2Relation
        && ((DataSourceV2Relation) table).table() instanceof HudiSparkTable) {
      return (DataSourceV2Relation) table;
    }
    return null;
  }

  /** {@code p1 <=> 'a' AND p2 <=> 'b'} over partition columns only, as ResolveInsertInto emits. */
  private static boolean isStaticPartitionPredicate(Expression expr, HudiSparkTable table) {
    if (expr instanceof And) {
      And and = (And) expr;
      return isStaticPartitionPredicate(and.left(), table)
          && isStaticPartitionPredicate(and.right(), table);
    }
    Expression left = null;
    Expression right = null;
    if (expr instanceof EqualNullSafe) {
      left = ((EqualNullSafe) expr).left();
      right = ((EqualNullSafe) expr).right();
    } else if (expr instanceof EqualTo) {
      left = ((EqualTo) expr).left();
      right = ((EqualTo) expr).right();
    } else {
      return false;
    }
    if (!(left instanceof AttributeReference) || !right.foldable()) {
      return false;
    }
    return table.context().getPartitionFields().contains(((AttributeReference) left).name());
  }
}
