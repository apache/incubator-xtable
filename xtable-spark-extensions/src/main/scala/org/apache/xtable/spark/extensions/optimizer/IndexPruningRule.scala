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
 
package org.apache.xtable.spark.extensions.optimizer

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  And,
  Attribute,
  AttributeReference,
  EqualTo,
  Expression,
  In,
  InSet,
  Literal
}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.iceberg.{FileScanTask, Table}
import org.apache.iceberg.expressions.{Expression => IcebergExpression, Expressions}
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.Type.TypeID

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.xtable.hudi.index.HudiBackedSecondaryIndexReader
import org.apache.xtable.spark.extensions.{IndexDefinition, XTableIndexConf}

/**
 * Uses an XTable index to read only the files that hold the keys of a filter such as
 * {@code col = 'x'} or {@code col IN ('x', 'y')} on an indexed column of an Iceberg table.
 *
 * The rule looks the keys up in the index, keeps the files that Iceberg's own partition and min/max
 * pruning also keeps, and stages them as the task set of the scan. The filter stays in the plan, so
 * the query returns the same rows as without the rule. The rule leaves the plan unchanged when the
 * index is behind the table, when the query reads an older snapshot or a branch, or when the scan
 * is too small for a lookup to pay off.
 */
case class IndexPruningRule(spark: SparkSession) extends Rule[LogicalPlan] {
  private val listenerRegistered = new AtomicBoolean(false)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!XTableIndexConf.pruningEnabled(conf)) {
      return plan
    }
    plan.transformDown {
      case filter @ Filter(
            condition,
            relation @ DataSourceV2Relation(table: SparkTable, _, _, _, options))
          if !options.containsKey(StagedScans.ScanTaskSetIdOption) &&
            table.snapshotId() == null && table.branch() == null =>
        try {
          prune(filter, condition, relation, table).getOrElse(filter)
        } catch {
          case NonFatal(e) =>
            logWarning(s"Not using the XTable index for ${table.name()}", e)
            filter
        }
    }
  }

  private def prune(
      filter: Filter,
      condition: Expression,
      relation: DataSourceV2Relation,
      sparkTable: SparkTable): Option[LogicalPlan] = {
    val definitions = IndexDefinition.fromProperties(sparkTable.properties())
    val table = sparkTable.table()
    for {
      (definition, keys) <- IndexPruningRule
        .findIndexedKeys(condition, relation.output, definitions, conf.resolver)
      if withinKeyLimit(table, definition, keys)
      snapshot <- Option(table.currentSnapshot())
      index = IndexDefinition.newIndex(
        spark,
        table,
        relation.catalog.map(_.name()),
        relation.identifier,
        definition)
      reader = index.reader
      if isCurrent(table, definition, reader.getLastSyncedSourceIdentifier, snapshot.snapshotId())
      // the files a scan reads after Iceberg's partition and min/max pruning
      candidates = planFiles(table, snapshot.snapshotId(), definition.column, keys)
      if isLargeEnough(table, definition, candidates)
    } yield {
      val keyColumn = spark
        .createDataset(keys.map(_.toString).distinct.asJava)(Encoders.STRING)
        .toDF(definition.column)
      // collect through the RDD, so the lookup runs as plain Spark jobs inside the planning query
      val indexedFiles = reader
        .lookup(index.sourceTable, keyColumn, definition.column)
        .select(HudiBackedSecondaryIndexReader.FILE_COLUMN)
        .rdd
        .map(_.getString(0))
        .distinct()
        .collect()
        .map(IndexPruningRule.normalizePath)
        .toSet
      val tasks = candidates.filter(task =>
        indexedFiles.contains(IndexPruningRule.normalizePath(task.file().location())))
      logInfo(
        s"Using index ${definition.name} of ${table.name()} for ${keys.size} keys: reading " +
          s"${tasks.size} of ${candidates.size} candidate files")
      if (tasks.isEmpty) {
        filter.copy(child = LocalRelation(relation.output, Nil, isStreaming = false))
      } else {
        registerCleanupListener()
        val setId = StagedScans.stage(table, tasks)
        val stagedOptions = new java.util.HashMap[String, String](options(relation))
        stagedOptions.put(StagedScans.ScanTaskSetIdOption, setId)
        filter.copy(child = relation.copy(options = new CaseInsensitiveStringMap(stagedOptions)))
      }
    }
  }

  private def withinKeyLimit(table: Table, definition: IndexDefinition, keys: Seq[Any]): Boolean = {
    val withinLimit = keys.size <= XTableIndexConf.maxKeys(conf)
    if (!withinLimit) {
      logInfo(s"Not using index ${definition.name} of ${table.name()}: ${keys.size} keys")
    }
    withinLimit
  }

  private def isCurrent(
      table: Table,
      definition: IndexDefinition,
      syncedSnapshot: java.util.Optional[String],
      currentSnapshot: Long): Boolean = {
    val current = syncedSnapshot.isPresent && syncedSnapshot.get() == currentSnapshot.toString
    if (!current) {
      logInfo(
        s"Not using index ${definition.name} of ${table.name()}: it is synced to snapshot " +
          s"${syncedSnapshot.orElse("none")}, but the table is at $currentSnapshot")
    }
    current
  }

  private def isLargeEnough(
      table: Table,
      definition: IndexDefinition,
      candidates: Seq[FileScanTask]): Boolean = {
    val candidateBytes = candidates.map(_.file().fileSizeInBytes()).sum
    val largeEnough = candidates.size >= XTableIndexConf.minCandidateFiles(conf) ||
      candidateBytes >= XTableIndexConf.minCandidateBytes(conf)
    if (!largeEnough) {
      logInfo(
        s"Not using index ${definition.name} of ${table.name()}: the scan reads only " +
          s"${candidates.size} files of $candidateBytes bytes")
    }
    largeEnough
  }

  private def options(relation: DataSourceV2Relation): java.util.Map[String, String] =
    relation.options.asCaseSensitiveMap()

  private def planFiles(
      table: Table,
      snapshotId: Long,
      column: String,
      keys: Seq[Any]): Seq[FileScanTask] = {
    val fieldType = table.schema().findField(column).`type`().typeId()
    val values = keys.map(IndexPruningRule.toIcebergValue(_, fieldType))
    val predicate: IcebergExpression =
      if (values.size == 1) Expressions.equal(column, values.head)
      else Expressions.in(column, values: _*)
    val files = table
      .newScan()
      .useSnapshot(snapshotId)
      .caseSensitive(conf.caseSensitiveAnalysis)
      .filter(predicate)
      .planFiles()
    try files.asScala.toList
    finally files.close()
  }

  // registered on first use, because the session state does not exist yet when the rule is built
  private def registerCleanupListener(): Unit = {
    if (listenerRegistered.compareAndSet(false, true)) {
      spark.listenerManager.register(new StagedScanCleanupListener)
    }
  }
}

object IndexPruningRule {

  /**
   * Finds a conjunct of the condition that compares an indexed column with literals, and returns
   * the index and the non-null literal values in Spark's internal representation.
   */
  def findIndexedKeys(
      condition: Expression,
      output: Seq[Attribute],
      definitions: Seq[IndexDefinition],
      resolver: (String, String) => Boolean): Option[(IndexDefinition, Seq[Any])] = {
    def indexOn(attribute: AttributeReference): Option[IndexDefinition] =
      if (output.exists(_.exprId == attribute.exprId)) {
        definitions.find(definition => resolver(attribute.name, definition.column))
      } else {
        None
      }

    splitConjuncts(condition).iterator
      .flatMap {
        case EqualTo(attribute: AttributeReference, Literal(value, _)) if value != null =>
          indexOn(attribute).map(_ -> Seq(value))
        case EqualTo(Literal(value, _), attribute: AttributeReference) if value != null =>
          indexOn(attribute).map(_ -> Seq(value))
        case In(attribute: AttributeReference, values) if values.forall(_.isInstanceOf[Literal]) =>
          val keys = values.map(_.asInstanceOf[Literal].value).filter(_ != null)
          indexOn(attribute).map(_ -> keys)
        case InSet(attribute: AttributeReference, values) =>
          indexOn(attribute).map(_ -> values.filter(_ != null).toSeq)
        case _ => None
      }
      .find(_._2.nonEmpty)
  }

  private def splitConjuncts(condition: Expression): Seq[Expression] = condition match {
    case And(left, right) => splitConjuncts(left) ++ splitConjuncts(right)
    case other => Seq(other)
  }

  /** Converts a literal value from Spark's internal representation for an Iceberg expression. */
  def toIcebergValue(value: Any, typeId: TypeID): AnyRef = (value, typeId) match {
    case (v: Int, TypeID.INTEGER) => Int.box(v)
    case (v: Int, TypeID.LONG) => Long.box(v.toLong)
    case (v: Long, TypeID.LONG) => Long.box(v)
    case (v, TypeID.STRING) => v.toString
    case _ =>
      throw new IllegalArgumentException(s"Unsupported key $value for a column of type $typeId")
  }

  /** Paths from the index and from Iceberg can differ in scheme, so both sides compare the path. */
  def normalizePath(path: String): String = new Path(path).toUri.getPath
}
