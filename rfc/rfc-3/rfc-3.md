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
- @vinothchandar
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

### What ships in v1

This PR delivers the **thin bundle plus the standalone `spark-submit` entry point
`XTableSparkSync`** — enough to run a relocated, in-cluster sync from one command. The config-only,
driver-side auto-sync (`XTableSyncListener` registered via `spark.sql.queryExecutionListeners`, with
`XTableSparkConfig` and `PlanTargetResolver`) is the headline on-ramp this RFC designs toward, but it
is **deferred to a follow-up PR** — those classes are not in v1. The listener design is retained
below as the target so the entry point and config schema stay aligned.

### Supported formats and engine versions (v1)

The bundle can read from Hudi, Iceberg, Delta, Paimon and Parquet, and write to Hudi, Iceberg and
Delta. Paimon and Parquet are **source-only** (there is no Paimon/Parquet `ConversionTarget`), so they
are valid for `--sourceformat` but not `--targets`. Syncing a format to itself is a no-op and not a
supported combination.

Source \ Target:

- Hudi source: to Iceberg yes, to Delta yes, to Hudi no (self)
- Iceberg source: to Hudi yes, to Delta yes, to Iceberg no (self)
- Delta source: to Hudi yes, to Iceberg yes, to Delta no (self)
- Paimon source (read-only): to Hudi yes, to Iceberg yes, to Delta yes
- Parquet source (read-only): to Hudi yes, to Iceberg yes, to Delta yes

Engines are **provided by the cluster**, not bundled — the user supplies the engine jars on the
classpath (see Packaging / classpath notes below). This bundle targets the Spark 3.4 line, pinned to a
mutually-compatible engine set: Spark 3.4.x, Scala 2.12, Hudi 0.14.0, Iceberg 1.9.2, Delta 2.4.0.
Delta 2.4.0 is Spark-3.4-only, which is why the line is pinned to Spark 3.4 rather than 3.5. The
spark-submit smoke test (`ITXTableSparkRuntimeBundle`) currently exercises one case per direction for
Hudi/Iceberg/Delta; Paimon and Parquet sources are wired but not yet covered by the smoke test.

### Activation — v1 (CLI entry point)

```
spark-submit \
  --jars /path/to/xtable-spark-runtime_2.12-<ver>.jar \
  --class org.apache.xtable.spark.XTableSparkSync \
  /path/to/xtable-spark-runtime_2.12-<ver>.jar \
  --basepath /warehouse/db/orders --sourceformat HUDI --targets ICEBERG,DELTA --tablename orders
```

Because the shaded jar is the **main artifact with a dependency-reduced POM** (see Packaging),
`--packages org.apache.xtable:xtable-spark-runtime_2.12:<ver>` resolves the same relocated bundle and
pulls no un-relocated transitive deps; `--jars` with the built jar is equivalent.

**Engine Avro / classpath placement (required).** The bundle does not carry avro/parquet — it uses
the engine's. Iceberg 1.9.2 compiles against Avro >= 1.12, newer than the Avro Spark 3.4 ships (1.11),
so the engine's Avro must win on the classpath. Supply the engine jars on a **flat** classpath
(`spark.driver.extraClassPath`/`spark.executor.extraClassPath`, or `--jars`), not layered under a
child classloader: `--packages` puts a second Avro in a child loader, which breaks cross-boundary Avro
casts (`NoSuchMethodError`/`ClassCastException`). This is why the bundle smoke test builds a single
flat `spark.driver.extraClassPath`.

### Activation — v1 (from application code)

The same sync can be triggered from an existing Spark job (Scala/Java or PySpark) instead of a
separate `spark-submit`, reusing the active `SparkSession`'s Hadoop configuration. This is the
programmatic equivalent of the CLI above and runs after your own write completes.

Scala / Java:

```scala
import org.apache.xtable.spark.{TableSyncSpec, XTableSyncService}

// ... after writing the source table with `spark` ...
val spec = TableSyncSpec.builder()
  .key("orders")
  .basePath("/warehouse/db/orders")
  .sourceFormat("HUDI")
  .targets(java.util.Arrays.asList("ICEBERG", "DELTA"))
  .build()

new XTableSyncService().sync(spec, spark.sparkContext.hadoopConfiguration)
```

PySpark (via the JVM gateway, with the bundle on the driver classpath):

```python
# after writing the source table with `spark` ...
jvm = spark._jvm
spec = (jvm.org.apache.xtable.spark.TableSyncSpec.builder()
        .key("orders")
        .basePath("/warehouse/db/orders")
        .sourceFormat("HUDI")
        .targets(jvm.java.util.Arrays.asList("ICEBERG", "DELTA"))
        .build())
hadoop_conf = spark._jsc.hadoopConfiguration()
jvm.org.apache.xtable.spark.XTableSyncService().sync(spec, hadoop_conf)
```

These examples, plus a `--packages`/`--jars` quickstart, should be folded into the docs-site
quickstart in a docs follow-up.

### Planned (follow-up): config-only listener activation

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

Both path-based and name-based table selection are planned for the listener on-ramp.

### Components

Shipped in v1:
- **`XTableSyncService`** — for one `TableSyncSpec`: build `SourceTable` + `TargetTable`s +
  `ConversionConfig(syncMode=INCREMENTAL)`, pick the source provider via a small
  `sourceProviderFor(format)` factory, and run `ConversionController.sync(...)`. Mirrors `XTableSyncTool`.
- **`TableSyncSpec`** — immutable description of one table to sync (key, basePath, dataPath,
  namespace, sourceFormat, targets).
- **`XTableSparkSync`** — the standalone `spark-submit` entry point. Parses CLI args (commons-cli)
  into a `TableSyncSpec` and drives `XTableSyncService`.

Planned (follow-up, **not in v1**):
- **`XTableSparkConfig`** — parse `SparkConf`/`Map` (+ optional `SparkSession` for name resolution)
  into `List<TableSyncSpec>`, validating required keys and failing fast with clear messages.
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

### Execution model: synchronous and stateless (planned listener)

The sync watermark already lives in the target's `TableSyncMetadata`, and sync is incremental +
idempotent, so a client-side dirty/pending/single-flight structure would only duplicate authoritative
state and could not be more correct. Therefore the listener keeps no execution state.

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

Scala-suffixed artifactId. **Spark, Hadoop, and the engine libraries (Hudi / Iceberg / Delta) are all
`provided`** — the user brings their own engine versions via the cluster or the submit
packages/jars flags, and the thin XTable bundle stays compatible across engine versions (Hudi 1.1 /
1.2, etc.). The `maven-shade-plugin` uses a **curated `artifactSet` allowlist**: only XTable's own
modules (`xtable-api`, `xtable-core`, `xtable-hudi-support-utils`) plus the libraries XTable uses
*purely internally* — `guava` (and its `failureaccess`) and `commons-cli` for the CLI — which are
relocated under `org.apache.xtable.shaded.*`. The allowlist is include-only, so its implicit
"provided by the cluster" assumptions (e.g. `jackson-datatype-jsr310` via `xtable-api`,
`log4j-1.2-api` via `xtable-core`) are documented in the pom; a `dependency:analyze`/enforcer gate to
keep it from silently under-bundling is a tracked follow-up.

The shaded jar is published as the **main artifact with a dependency-reduced POM**
(`shadedArtifactAttached=false`, `createDependencyReducedPom=true`) — the same shape as
`iceberg-spark-runtime` and `hudi-spark-bundle`, so resolving the coordinate (Maven dep or
`--packages`) yields the relocated bundle and no un-relocated transitive deps. Because the jar
relocates and ships `guava`/`failureaccess`/`commons-cli` (all Apache-2.0), its `LICENSE`/`NOTICE`
carry those attributions (`LICENSE-bundled`/`NOTICE-bundled` via `IncludeResourceTransformer`).
`protobuf` is **not** bundled — it resolves as `provided` (supplied by the engines), so it needs no
relocation or notice.

Critically, libraries XTable **exchanges across the engine API boundary must not be relocated or
bundled** — e.g. Hudi returns a real `org.apache.avro.Schema`, so a relocated/bundled `avro` in the
XTable jar produces a `NoSuchMethodError` at runtime. `avro` / `parquet` / `jackson` / `commons` are
therefore left to the provided runtime. Resulting bundle: **~3.8 MB**.

Because `ConversionTargetFactory` discovers targets via `ServiceLoader` and would otherwise fail if
any registered engine (e.g. Delta) is absent, target discovery is made **resilient**: providers whose
engine library is not on the classpath are skipped, so a user can run with just the engines they use
(e.g. Hudi + Iceberg, no Delta). This is a small `xtable-core` change and is validated by the bundle
smoke test.

A standalone `spark-submit` entry point (`XTableSparkSync`, the RunSync-equivalent for this bundle)
lets the shaded jar run a sync directly and is what the jar-validation test drives.

## Rollout/Adoption Plan

- **No breaking changes.** This is a new, additive module; existing `RunSync`/`xtable-utilities` and
  `xtable-core` behavior is unchanged.
- **No impact on existing users** unless they opt in by adding the jar and setting `spark.xtable.*`.
- **Spark support:** this bundle targets the Spark line of its engine build (Spark 3.4, Scala 2.12),
  with newer Spark lines as follow-ups. The per-Spark-version coupling
  comes from the Delta-on-Spark path; making Delta Kernel the default (tracked separately) would let a
  single bundle serve multiple Spark lines.
- **Deferred (follow-up RFCs/PRs):** the config-only `XTableSyncListener` on-ramp, a
  `StreamingQueryListener` variant, a `CALL xtable.sync(...)` SQL procedure, and the async execution
  opt-in.

## Test Plan

- **Bundle smoke test (`ITXTableSparkRuntimeBundle`), v1:** `spark-submit` the shaded main jar via
  `XTableSparkSync` for one case per direction (Hudi↔Iceberg, Hudi↔Delta), supplying the engines on a
  **flat** `spark.driver.extraClassPath` (single Avro on one loader), and assert the target is
  data-equivalent to the source by reading both back through Spark (Hudi with metadata enabled;
  Iceberg non-vectorized to avoid a cross-engine timestamp Arrow-cast). `SPARK_HOME`-gated. This
  validates that the relocated jar is self-contained and runs a real sync.
- **Planned (with the listener follow-up):** unit tests for `XTableSparkConfig` parsing (path- and
  name-based, missing-key errors) and `PlanTargetResolver` path extraction; and an
  `ITXTableSyncListener` embedded `local[*]` test that registers the listener via
  `spark.sql.queryExecutionListeners` and poll-asserts both targets appear after a write.
- **Bundle sanity:** package the module and inspect the shaded main jar — confirm it is a few MB
  (~3.8 MB), `org.apache.spark`/`org.apache.hadoop`/`org.apache.avro` are absent, and the relocated
  libraries live under `org.apache.xtable.shaded.*`.
