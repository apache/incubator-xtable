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

import org.apache.spark.sql.internal.SQLConf

/** Session settings for the XTable index rule. */
object XTableIndexConf {
  val PruningEnabled = "spark.xtable.index.pruning.enabled"

  /**
   * The rule uses the index only when the files left after Iceberg's partition and min/max
   * pruning reach this count, or their size reaches [[MinCandidateBytes]]. A lookup runs a Spark
   * job, so it costs more than it saves on a small scan.
   */
  val MinCandidateFiles = "spark.xtable.index.pruning.minCandidateFiles"
  val MinCandidateBytes = "spark.xtable.index.pruning.minCandidateBytes"

  /** The rule does not use the index for a filter with more keys than this. */
  val MaxKeys = "spark.xtable.index.pruning.maxKeys"

  private val DefaultMinCandidateFiles = 32L
  private val DefaultMinCandidateBytes = 1024L * 1024 * 1024
  private val DefaultMaxKeys = 10000L

  def pruningEnabled(conf: SQLConf): Boolean =
    conf.getConfString(PruningEnabled, "true").toBoolean

  def minCandidateFiles(conf: SQLConf): Long =
    conf.getConfString(MinCandidateFiles, DefaultMinCandidateFiles.toString).toLong

  def minCandidateBytes(conf: SQLConf): Long =
    conf.getConfString(MinCandidateBytes, DefaultMinCandidateBytes.toString).toLong

  def maxKeys(conf: SQLConf): Long =
    conf.getConfString(MaxKeys, DefaultMaxKeys.toString).toLong
}
