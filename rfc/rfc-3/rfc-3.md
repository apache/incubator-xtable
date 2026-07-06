<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# RFC-3: xtable-spark-runtime - in-job metadata sync via a thin Spark bundle

## Proposers

- @vinishjail97

## Approvers
- Anyone from the XTable community can approve/add feedback.

## Status

GH Feature Request: https://github.com/apache/incubator-xtable/issues/836

> Please keep the status updated in `rfc/README.md`.

## Abstract

XTable's modules are published to Maven, so a user can depend on `xtable-core` and assemble their own
runtime today. But there is no maintained, thin, drop-in artifact — everyone re-solves the same
shading/classpath problem, or uses `xtable-utilities`, an unshaded ~1 GB fat jar that is not practical
to add to a Spark job and requires running a separate `RunSync` process with YAML config files.

XTable conversion is metadata-only and lightweight, so the common case — "I already write this table
with Spark, keep it in sync in other formats" — should be a one-dependency, config-only addition to an
existing pipeline. This RFC proposes `xtable-spark-runtime`: a thin, relocated Spark bundle that
registers a driver-side listener and, after each successful write to a source table, runs an
incremental `ConversionController.sync(...)` for the configured targets. It complements — does not
replace — the standalone CLI (`RunSync`).

## Background

Everything needed to run a sync already exists in the engine; the gap is purely packaging and a trigger:

- The entire sync entry surface is Hadoop-`Configuration`-only: `new ConversionController(conf).sync(config, sourceProvider)`.
  The caller supplies only the **source** `ConversionSourceProvider`; target providers are created
  internally by `ConversionTargetFactory` (`ConversionController.java`). Source providers instantiate
  directly (`new HudiConversionSourceProvider()` / `IcebergConversionSourceProvider` /
  `DeltaConversionSourceProvider`) and take `.init(conf)`.
- `SyncMode.INCREMENTAL` is self-healing: `ConversionController` auto-falls back to a full snapshot
  per-target when there is no prior sync metadata or incremental is not safe (`isIncrementalSyncSufficient`).
  First sync is effectively full; subsequent syncs are incremental.
- The sync watermark (last-synced instant, pending commits) is persisted in the **target's**
  `TableSyncMetadata`. There is no additional state to keep on the client side.
- `xtable-hudi-support-extensions` is a pure-Java precedent for a Spark-adjacent module whose
  `XTableSyncTool` already builds `SourceTable`/`TargetTable` and calls `ConversionController.sync(...)`;
  it declares Spark deps as `provided` and uses a Scala-suffixed artifactId.
- Delta conversion currently runs Delta-on-Spark in-process (`delta-core`); `DeltaConversionUtils.buildSparkSession`
  uses `SparkSession.builder()...getOrCreate()` with no `master`, so it reuses the host job's active
  session rather than starting a second `SparkContext`. (A Spark-free Delta **Kernel** path exists and,
  once it becomes the default, removes the per-Spark-version coupling — tracked separately.)

## Implementation

A new **pure-Java** module `xtable-spark-runtime_${scala.binary.version}` (package `org.apache.xtable.spark`).

### Activation (config only)

```
spark-submit --packages org.apache.xtable:xtable-spark-runtime_2.12:<ver> \
  --conf spark.sql.queryExecutionListeners=org.apache.xtable.spark.XTableSyncListener \
  --conf spark.xtable.tables=orders \
  --conf spark.xtable.orders.basePath=/warehouse/db/orders \
  --conf spark.xtable.orders.sourceFormat=HUDI \
  --conf spark.xtable.orders.targets=ICEBERG,DELTA
```

Config schema (`spark.xtable.*`):
- `spark.xtable.tables` — comma-separated per-table keys (logical names).
- Per key `<k>`: `spark.xtable.<k>.basePath` (path-based) **or** `spark.xtable.<k>.sourceTable=db.table`
  (name-based; resolved to a base path via the active `SparkSession` catalog); plus
  `spark.xtable.<k>.sourceFormat` and `spark.xtable.<k>.targets` (comma list). Optional:
  `spark.xtable.<k>.dataPath`, `spark.xtable.<k>.namespace`.

Both path-based and name-based table selection are supported from the first release.

### Components

- **`TableSyncSpec`** — immutable description of one configured table (key, basePath, dataPath,
  namespace, sourceFormat, targets).
- **`XTableSparkConfig`** — parse `SparkConf`/`Map` (+ optional `SparkSession` for name resolution)
  into `List<TableSyncSpec>`, validating required keys and failing fast with clear messages.
- **`XTableSyncService`** — for one `TableSyncSpec`: build `SourceTable` + `TargetTable`s +
  `ConversionConfig(syncMode=INCREMENTAL)`, pick the source provider via a small
  `sourceProviderFor(format)` factory, and run `ConversionController.sync(...)`. Mirrors `XTableSyncTool`.
- **`PlanTargetResolver`** — best-effort extraction of the written output path from `qe.analyzed()`
  (`InsertIntoHadoopFsRelationCommand`, `SaveIntoDataSourceCommand`, and the DataSource-V2
  `AppendData`/`OverwriteByExpression` nodes used by Iceberg). Isolated and unit-testable; returns
  `Optional<String>`.
- **`XTableSyncListener implements QueryExecutionListener`** — **stateless** (only immutable parsed
  config). On `onSuccess`, resolve the written path (best-effort) and, if it matches a configured
  table (basePath equals or is a prefix of the written path), call `XTableSyncService.sync(...)`
  **inline (synchronous)**. Reads and writes to unconfigured tables are ignored — the resolver only
  returns a path for recognized write commands, so a read never triggers a sync. Per-table failures
  are caught (as `Throwable`) so one table can't stop the others or destabilize Spark's listener bus.
  `onFailure` logs only.

### Execution model: synchronous and stateless

The sync watermark already lives in the target's `TableSyncMetadata`, and sync is incremental +
idempotent, so a client-side dirty/pending/single-flight structure would only duplicate authoritative
state and could not be more correct. Therefore v1 keeps no execution state.

Spark delivers `QueryExecutionListener` callbacks asynchronously via the driver's `LiveListenerBus`
(off `SparkListenerSQLExecutionEnd`), not on the thread that ran `df.write` — so a listener cannot
block the write from returning, and the bus processes events on a single dispatch thread. Running the
sync inline on the callback therefore (a) needs no executor or locking, (b) gets single-flight per
table for free (the bus is single-threaded), and (c) completes before JVM exit for a normally
terminating job, because `SparkContext.stop()` drains the bus. A hard `kill -9` may skip a queued
callback; the next commit's sync self-heals it.

The cost of inline execution is that it occupies the bus dispatch thread for the sync duration —
acceptable for fast metadata-only syncs, which is the common case.

**Async / "when to trigger" is intentionally left to the user.** Whether to offload the sync to a
background thread, and even whether/when to trigger at all (only after the final write, batching
several writes, latency tolerance), depends on the job's DAG, which only the user knows. Async is an
opt-in follow-up (`spark.xtable.sync.async`), not the default.

### Packaging (thin bundle)

Modeled on `xtable-hudi-support-extensions`: Scala-suffixed artifactId; `xtable-core` bundled (compile);
Spark and Hadoop `provided` (present on the cluster). A `maven-shade-plugin` config relocates
`guava` / `jackson` / `protobuf` / `avro` / `commons` under `org.apache.xtable.shaded.*` and uses a
curated `artifactSet` allowlist (xtable modules + the format libraries) that **excludes**
Spark/Hadoop/Hive. Target size: tens of MB. The exact allowlist and relocation set are tuned by
building and inspecting the jar.

## Rollout/Adoption Plan

- **No breaking changes.** This is a new, additive module; existing `RunSync`/`xtable-utilities` and
  `xtable-core` behavior is unchanged.
- **No impact on existing users** unless they opt in by adding the jar and setting `spark.xtable.*`.
- **Spark support:** target Spark 3.5 first (Scala 2.12), with a Spark 4 upgrade as a follow-up. The
  per-Spark-version coupling comes from the Delta-on-Spark path; making Delta Kernel the default
  (tracked separately) would let a single bundle serve multiple Spark lines.
- **Deferred (follow-up RFCs/PRs):** a `StreamingQueryListener` variant, a `CALL xtable.sync(...)` SQL
  procedure, and the async execution opt-in.

## Test Plan

- **Unit:** `XTableSparkConfig` parsing (path- and name-based, missing-key errors);
  `PlanTargetResolver` path extraction from representative write plans (and empty result when
  unresolvable).
- **Integration (`ITXTableSyncListener`, embedded `local[*]`):** register the listener via
  `spark.sql.queryExecutionListeners`, configure a Hudi source with `targets=DELTA,ICEBERG`, write the
  table, then poll-with-timeout (callback delivery is async) and assert both targets by reading them
  back through Spark (`format("delta")` / `format("iceberg")`) and comparing row counts to the source,
  mirroring `ITConversionController.checkDatasetEquivalence`.
- **Bundle sanity:** package the module and inspect the shaded jar — confirm size is tens of MB,
  `org.apache.spark`/`org.apache.hadoop` are absent, and relocated libraries live under
  `org.apache.xtable.shaded.*`.
