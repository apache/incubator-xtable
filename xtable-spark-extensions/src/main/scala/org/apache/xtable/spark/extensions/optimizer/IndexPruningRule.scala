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

import org.apache.iceberg.{FileScanTask, Snapshot, Table}
import org.apache.iceberg.expressions.{Expression => IcebergExpression, Expressions}
import org.apache.iceberg.spark.SparkReadOptions
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.NestedField

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.xtable.hudi.index.HudiBackedSecondaryIndexReader
import org.apache.xtable.hudi.index.HudiBackedSecondaryIndexUtils.SyncedVersion
import org.apache.xtable.spark.extensions.{XTableIndex, XTableIndexConf}

/**
 * Uses an XTable index to read only the files that hold the keys of a filter such as
 * {@code col = 'x'} or {@code col IN ('x', 'y')} on an indexed column of an Iceberg table. The rule
 * runs for SQL queries and DataFrame reads alike.
 *
 * The rule looks the keys up in the index, keeps the files that Iceberg's own partition and min/max
 * pruning also keeps, and stages them as the task set of the scan. The filter stays in the plan, so
 * the query returns the same rows as without the rule. The rule leaves the plan unchanged when the
 * index is behind the table, when the query reads an older snapshot, a branch, a tag or the changes
 * between snapshots, or when the scan is too small for a lookup to pay off. The rule fails the query
 * when the indexed column was renamed after the index was synced.
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
          if !IndexPruningRule.SnapshotReadOptions.exists(options.containsKey) &&
            table.snapshotId() == null && table.branch() == null =>
        try {
          prune(filter, condition, relation, table).getOrElse(filter)
        } catch {
          case e: IndexedColumnRenamedException => throw e
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
    val keyFilters = IndexPruningRule.findKeyFilters(condition, relation.output)
    if (keyFilters.isEmpty) {
      return None
    }
    val table = sparkTable.table()
    val index =
      XTableIndex.of(spark, table, relation.catalog.map(_.name()), relation.identifier)
    for {
      (column, keys) <- findIndexedFilter(table, index, keyFilters)
      if withinKeyLimit(table, column, keys)
      snapshot <- Option(table.currentSnapshot())
      version = index.syncedVersion
      if isCurrent(table, column, version, snapshot.snapshotId())
      _ = checkNotRenamed(table, column, snapshot)
      // the files a scan reads after Iceberg's partition and min/max pruning
      candidates = planFiles(table, snapshot.snapshotId(), column, keys)
      if isLargeEnough(table, column, candidates)
      indexedFiles = lookup(index, column, keys)
      // A sync, rollback or rebuild that completes between the check above and the end of the
      // lookup can point the index at other files than the candidates, even at the same snapshot.
      // Each of them completes a new action on the index, so an equal version after the lookup
      // shows that the lookup read the checked version.
      if isUnchanged(table, column, version, index.syncedVersion)
    } yield {
      val tasks = candidates.filter(task =>
        indexedFiles.contains(IndexPruningRule.normalizePath(task.file().location())))
      if (tasks.isEmpty) {
        logInfo(
          s"Using the index of ${table.name()} on $column for ${keys.size} keys: no file of " +
            s"${candidates.size} candidate files holds them")
        filter.copy(child = LocalRelation(relation.output, Nil, isStreaming = false))
      } else {
        StagedScans.stage(table, tasks) match {
          case Some(setId) =>
            logInfo(
              s"Using the index of ${table.name()} on $column for ${keys.size} keys: reading " +
                s"${tasks.size} of ${candidates.size} candidate files")
            registerCleanupListener()
            val stagedOptions = new java.util.HashMap[String, String](options(relation))
            stagedOptions.put(StagedScans.ScanTaskSetIdOption, setId)
            filter.copy(child =
              relation.copy(options = new CaseInsensitiveStringMap(stagedOptions)))
          case None =>
            logWarning(
              s"Not using the index of ${table.name()} on $column: too many staged scans are " +
                "live, which happens when plans are optimized but not run")
            filter
        }
      }
    }
  }

  /**
   * Returns the first filter on an indexed column, with the name of the column in the table
   * schema. Hudi records the indexed column under that name.
   */
  private def findIndexedFilter(
      table: Table,
      index: XTableIndex,
      keyFilters: Seq[(String, Seq[Any])]): Option[(String, Seq[Any])] = {
    val schema = table.schema()
    keyFilters.iterator
      .flatMap { case (attribute, keys) =>
        val field: NestedField =
          if (conf.caseSensitiveAnalysis) schema.findField(attribute)
          else schema.caseInsensitiveFindField(attribute)
        Option(field)
          .filter(field => XTableIndex.SupportedTypes.contains(field.`type`().typeId()))
          .map(field => field.name() -> keys)
      }
      .find { case (column, _) => index.isIndexed(column) }
  }

  /**
   * Fails when the column names another field than in the schema of the synced snapshot. A rename
   * keeps the snapshot, so the index stays current, but it keys the column by name and then holds
   * the values of another field.
   */
  private def checkNotRenamed(table: Table, column: String, snapshot: Snapshot): Unit = {
    val syncedField =
      Option(snapshot.schemaId())
        .flatMap(id => Option(table.schemas().get(id)))
        .map(_.findField(column))
    if (syncedField.exists(field =>
        field == null || field.fieldId() != table.schema().findField(column).fieldId())) {
      throw new IndexedColumnRenamedException(
        s"The index of ${table.name()} on $column was synced when $column named another field. " +
          s"Rebuild the index after the next commit to the table, or set " +
          s"${XTableIndexConf.PruningEnabled} to false.")
    }
  }

  private def lookup(index: XTableIndex, column: String, keys: Seq[Any]): Set[String] = {
    IndexPruningRule.beforeLookup()
    val keyColumn = spark
      .createDataset(keys.map(_.toString).distinct.asJava)(Encoders.STRING)
      .toDF(column)
    // collect through the RDD, so the lookup runs as plain Spark jobs inside the planning query
    val files = index.reader
      .lookup(index.sourceTable, keyColumn, column)
      .select(HudiBackedSecondaryIndexReader.FILE_COLUMN)
      .rdd
      .map(_.getString(0))
      .distinct()
      .collect()
      .map(IndexPruningRule.normalizePath)
      .toSet
    IndexPruningRule.afterLookup()
    files
  }

  private def withinKeyLimit(table: Table, column: String, keys: Seq[Any]): Boolean = {
    val withinLimit = keys.size <= XTableIndexConf.maxKeys(conf)
    if (!withinLimit) {
      logInfo(s"Not using the index of ${table.name()} on $column: ${keys.size} keys")
    }
    withinLimit
  }

  private def isCurrent(
      table: Table,
      column: String,
      version: Option[SyncedVersion],
      currentSnapshot: Long): Boolean = {
    val syncedSnapshot = version.map(_.getSourceIdentifier)
    val current = syncedSnapshot.contains(currentSnapshot.toString)
    if (!current) {
      logWarning(
        s"Not using the index of ${table.name()} on $column: it is synced to snapshot " +
          s"${syncedSnapshot.getOrElse("none")}, but the table is at $currentSnapshot. Run the " +
          "XTable sync of the table to update the index")
    }
    current
  }

  private def isUnchanged(
      table: Table,
      column: String,
      checkedVersion: Option[SyncedVersion],
      versionAfterLookup: Option[SyncedVersion]): Boolean = {
    val unchanged = versionAfterLookup == checkedVersion
    if (!unchanged) {
      logInfo(
        s"Not using the index of ${table.name()} on $column: it changed from $checkedVersion " +
          s"to ${versionAfterLookup.getOrElse("none")} during the lookup")
    }
    unchanged
  }

  private def isLargeEnough(
      table: Table,
      column: String,
      candidates: Seq[FileScanTask]): Boolean = {
    val candidateBytes = candidates.map(_.file().fileSizeInBytes()).sum
    val largeEnough = candidates.size >= XTableIndexConf.minCandidateFiles(conf) ||
      candidateBytes >= XTableIndexConf.minCandidateBytes(conf)
    if (!largeEnough) {
      logInfo(
        s"Not using the index of ${table.name()} on $column: the scan reads only " +
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

/** Thrown when the indexed column was renamed after the index was synced. */
class IndexedColumnRenamedException(message: String) extends IllegalStateException(message)

object IndexPruningRule {

  /**
   * Read options that select other rows than the current snapshot of the table, or that already
   * select the tasks of the scan. The rule leaves scans with any of them unchanged.
   */
  val SnapshotReadOptions: Seq[String] = Seq(
    SparkReadOptions.SNAPSHOT_ID,
    SparkReadOptions.AS_OF_TIMESTAMP,
    SparkReadOptions.VERSION_AS_OF,
    SparkReadOptions.TIMESTAMP_AS_OF,
    SparkReadOptions.BRANCH,
    SparkReadOptions.TAG,
    SparkReadOptions.START_SNAPSHOT_ID,
    SparkReadOptions.END_SNAPSHOT_ID,
    SparkReadOptions.START_TIMESTAMP,
    SparkReadOptions.END_TIMESTAMP,
    StagedScans.ScanTaskSetIdOption
  )

  /** Runs after the freshness check and before the lookup. Tests use it to run a sync between. */
  @volatile private[extensions] var beforeLookup: () => Unit = () => ()

  /** Runs after the lookup and before the second check. Tests use it to change the index there. */
  @volatile private[extensions] var afterLookup: () => Unit = () => ()

  /**
   * Finds the conjuncts of the condition that compare a column of the relation with literals, and
   * returns the name of each column with its non-null literal values in Spark's internal
   * representation.
   */
  def findKeyFilters(condition: Expression, output: Seq[Attribute]): Seq[(String, Seq[Any])] = {
    def column(attribute: AttributeReference): Option[String] =
      if (output.exists(_.exprId == attribute.exprId)) Some(attribute.name) else None

    splitConjuncts(condition)
      .flatMap {
        case EqualTo(attribute: AttributeReference, Literal(value, _)) if value != null =>
          column(attribute).map(_ -> Seq(value))
        case EqualTo(Literal(value, _), attribute: AttributeReference) if value != null =>
          column(attribute).map(_ -> Seq(value))
        case In(attribute: AttributeReference, values) if values.forall(_.isInstanceOf[Literal]) =>
          val keys = values.map(_.asInstanceOf[Literal].value).filter(_ != null)
          column(attribute).map(_ -> keys)
        case InSet(attribute: AttributeReference, values) =>
          column(attribute).map(_ -> values.filter(_ != null).toSeq)
        case _ => None
      }
      .filter(_._2.nonEmpty)
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
