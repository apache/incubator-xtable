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

import java.util.function.Function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases$;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Not;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression;
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
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

  /**
   * Where in the analyzer the rule runs. Row-level commands are claimed during resolution, before
   * Spark's and Iceberg's own rewrites in the same batch get to them; appends and overwrites wait
   * for post-hoc, when their query has been aligned to the table schema.
   */
  public enum Phase {
    RESOLUTION,
    POST_HOC
  }

  private final SparkSession spark;
  private final Phase phase;

  public HudiIcebergWriteRule(SparkSession spark) {
    this(spark, Phase.POST_HOC);
  }

  public HudiIcebergWriteRule(SparkSession spark, Phase phase) {
    this.spark = spark;
    this.phase = phase;
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
    if (node instanceof ReplaceData) {
      // Spark 3.4 rewrites DELETE during resolution; the delete condition and the original
      // relation survive on the ReplaceData node, and its query has the shape Filter(NOT cond)
      ReplaceData replace = (ReplaceData) node;
      DataSourceV2Relation target =
          managedRowLevelTarget((LogicalPlan) replace.originalTable(), "DELETE");
      if (target == null) {
        return null;
      }
      if (isRemainingRowsFilter(replace.query(), replace.condition())) {
        return command(
            target, new Filter(replace.condition(), target), HudiWriteOperation.DELETE, null);
      }
      LOG.warn(
          "Rewritten row-level command on {} stays on the Iceberg writer: unrecognized shape {}",
          target.table().name(),
          replace.query().getClass().getSimpleName());
      return null;
    }
    if (phase == Phase.RESOLUTION
        && !(node instanceof UpdateTable)
        && !(node instanceof MergeIntoTable)) {
      return null;
    }
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
    if (node instanceof DeleteFromTable) {
      DeleteFromTable delete = (DeleteFromTable) node;
      DataSourceV2Relation target = managedRowLevelTarget(delete.table(), "DELETE");
      if (target == null) {
        return null;
      }
      LogicalPlan rows = new Filter(delete.condition(), target);
      return command(target, rows, HudiWriteOperation.DELETE, null);
    }
    if (node instanceof UpdateTable) {
      UpdateTable update = (UpdateTable) node;
      DataSourceV2Relation target = managedRowLevelTarget(update.table(), "UPDATE");
      if (target == null) {
        return null;
      }
      try {
        Function<Dataset<Row>, Dataset<Row>> transform =
            HudiRowLevelPlanner.update(spark, target, update);
        Expression condition =
            update.condition().isDefined() ? update.condition().get() : Literal.TrueLiteral();
        return command(target, new Filter(condition, target), HudiWriteOperation.UPSERT, transform);
      } catch (HudiRowLevelPlanner.Unsupported e) {
        LOG.warn(
            "UPDATE of {} stays on the Iceberg writer: {}", target.table().name(), e.getMessage());
        return null;
      }
    }
    if (node instanceof MergeIntoTable) {
      MergeIntoTable merge = (MergeIntoTable) node;
      DataSourceV2Relation target = managedRowLevelTarget(merge.targetTable(), "MERGE INTO");
      if (target == null || !merge.sourceTable().resolved()) {
        return null;
      }
      try {
        Function<Dataset<Row>, Dataset<Row>> transform =
            HudiRowLevelPlanner.merge(spark, target, merge);
        return command(target, merge.sourceTable(), HudiWriteOperation.UPSERT, transform);
      } catch (HudiRowLevelPlanner.Unsupported e) {
        LOG.warn(
            "MERGE INTO {} stays on the Iceberg writer: {}", target.table().name(), e.getMessage());
        return null;
      }
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
    return command(target, query, operation, null);
  }

  /** A row-level command targets a keyed managed table, possibly behind a subquery alias. */
  private DataSourceV2Relation managedRowLevelTarget(LogicalPlan table, String statement) {
    LogicalPlan unaliased = EliminateSubqueryAliases$.MODULE$.apply(table);
    if (!(unaliased instanceof DataSourceV2Relation)
        || !(((DataSourceV2Relation) unaliased).table() instanceof HudiSparkTable)) {
      return null;
    }
    DataSourceV2Relation target = (DataSourceV2Relation) unaliased;
    if (!((HudiSparkTable) target.table()).context().isKeyed()) {
      LOG.warn(
          "{} on {} stays on the Iceberg writer: the table has no identifier fields, so Hudi has "
              + "no record key to address rows by",
          statement,
          target.table().name());
      return null;
    }
    return target;
  }

  private LogicalPlan command(
      DataSourceV2Relation target,
      LogicalPlan query,
      HudiWriteOperation operation,
      Function<Dataset<Row>, Dataset<Row>> transform) {
    HudiSparkTable table = (HudiSparkTable) target.table();
    TableCatalog catalog = null;
    Option<CatalogPlugin> plugin = target.catalog();
    if (plugin.isDefined() && plugin.get() instanceof TableCatalog) {
      catalog = (TableCatalog) plugin.get();
    }
    Identifier identifier = target.identifier().isDefined() ? target.identifier().get() : null;
    LOG.info("Routing {} on {} through Hudi", operation, table.name());
    HudiIcebergRelation relation =
        new HudiIcebergRelation(spark, table, operation, catalog, identifier, transform);
    LogicalRelation logicalRelation = LogicalRelation$.MODULE$.apply(relation, false);
    boolean overwrite =
        operation == HudiWriteOperation.INSERT_OVERWRITE
            || operation == HudiWriteOperation.INSERT_OVERWRITE_TABLE;
    return new InsertIntoDataSourceCommand(logicalRelation, query, overwrite);
  }

  /**
   * The rows a rewritten DELETE keeps: {@code Filter(NOT cond, scan)} or, as Spark 3.4 writes it to
   * also keep rows where the condition is null, {@code Filter(NOT (cond <=> true), scan)}.
   */
  private static boolean isRemainingRowsFilter(LogicalPlan query, Expression deleteCondition) {
    if (!(query instanceof Filter) || !(((Filter) query).condition() instanceof Not)) {
      return false;
    }
    Expression kept = ((Not) ((Filter) query).condition()).child();
    if (kept instanceof EqualNullSafe
        && ((EqualNullSafe) kept).right() instanceof Literal
        && Boolean.TRUE.equals(((Literal) ((EqualNullSafe) kept).right()).value())) {
      kept = ((EqualNullSafe) kept).left();
    }
    return kept.semanticEquals(deleteCondition);
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
