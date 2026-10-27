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

import java.util.UUID

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.{
  DataSourceV2Relation,
  DataSourceV2ScanRelation
}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.iceberg.{FileScanTask, Table}
import org.apache.iceberg.spark.ScanTaskSetManager

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Stages the file scan tasks that an index lookup selects. Iceberg reads a staged task set when a
 * scan has the {@code scan-task-set-id} option. Staged tasks live in a process wide registry until
 * they are removed, so this object removes them when the query that uses them ends. Iceberg reads
 * the tasks of a scan only when Spark plans its partitions, so a set is never removed before its
 * query ends. Plans that are only explained never end and leave their sets staged, so this object
 * stages no new set once too many are live, and the scans that would use them read the table as
 * usual.
 */
object StagedScans {
  val ScanTaskSetIdOption = "scan-task-set-id"

  val MaxLiveSets = 1000

  private val liveSets = new java.util.HashMap[String, Table]()

  /** Stages the tasks and returns the id of their set, or nothing when too many sets are live. */
  def stage(table: Table, tasks: Seq[FileScanTask]): Option[String] = {
    val setId = UUID.randomUUID().toString
    val reserved = liveSets.synchronized {
      liveSets.size() < MaxLiveSets && liveSets.put(setId, table) == null
    }
    if (reserved) {
      ScanTaskSetManager.get().stageTasks(table, setId, tasks.asJava)
      Some(setId)
    } else {
      None
    }
  }

  def release(setId: String): Unit = {
    val table = liveSets.synchronized(liveSets.remove(setId))
    if (table != null) {
      removeTasks(table, setId)
    }
  }

  def liveSetIds: Set[String] = liveSets.synchronized(liveSets.keySet().asScala.toSet)

  /** Releases the task sets that the scans of a plan use. */
  def releaseAll(plan: LogicalPlan): Unit = {
    def setIdOf(relation: DataSourceV2Relation): Option[String] =
      Option(relation.options.get(ScanTaskSetIdOption))
    plan
      .collectWithSubqueries {
        case scan: DataSourceV2ScanRelation => setIdOf(scan.relation)
        case relation: DataSourceV2Relation => setIdOf(relation)
      }
      .flatten
      .foreach(release)
  }

  private def removeTasks(table: Table, setId: String): Unit =
    ScanTaskSetManager.get().removeTasks[FileScanTask](table, setId)
}

/** Releases the staged task sets of a query once it ends. */
class StagedScanCleanupListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    release(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    release(qe)

  // a query that failed during optimization has no optimized plan, and nothing was staged for it
  private def release(qe: QueryExecution): Unit =
    Try(qe.optimizedPlan).foreach(StagedScans.releaseAll)
}
