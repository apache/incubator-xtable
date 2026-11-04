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
# RFC-4: A web UI for table format conversion in `xtable-service`

## Proposers

- @rangareddy

## Approvers

- Anyone from the XTable community can approve/add feedback.

## Status

GH Feature Request: https://github.com/apache/incubator-xtable/issues/497

> Please keep the status updated in `rfc/README.md`.

RFC-3 is already referenced by `xtable-spark-runtime/pom.xml` on the 0.4.x line for the
config-only `QueryExecutionListener` on-ramp, so this proposal takes RFC-4.

## Abstract

Converting a table with XTable today means hand-writing a YAML config and running `RunSync`, or
hand-typing a JSON body into an API client. The two screenshots checked into
`xtable-service/examples/` are Postman, which is the honest picture of the current interface.

Issue #497 asks for a UI that removes the config file, shows the status and progress of a
conversion while it runs, and makes the logs readable without going to the machine that ran it.

This RFC proposes serving that UI from the existing `xtable-service` module, and describes the
backend work it needs. Two of the three things the issue asks for, progress and logs, cannot be
built on the endpoint that exists today, so most of this document is about the API rather than the
screens.

A visual proposal covering six screens was attached to #497 on 2026-08-03. This RFC is the
engineering counterpart to it and supersedes its API sketch where the two differ, for the reason
given under [Asynchronous conversion](#asynchronous-conversion).

## Background

### What exists today

`xtable-service` is a Quarkus application (`quarkus.platform.version` `3.2.12.Final`, pinned in the
root `pom.xml` with the comment `compatible with Java 11`). It exposes exactly one endpoint,
declared in `ConversionResource`:

```java
@Path("/v1/conversion")
public class ConversionResource {
  @POST
  @Path("/table")
  @Blocking
  public ConvertTableResponse convertTable(ConvertTableRequest convertTableRequest) { ... }
}
```

`ConversionService.convertTable` builds a `SourceTable` and one `TargetTable` per requested format,
calls `ConversionController.sync`, then reads each target back to return its schema. The call is
synchronous and returns only a final result.

Four properties of the current code constrain any UI built on it:

1. **Three formats are reachable, not five.** `ConversionService.initSourceProviders` registers
   providers for `HUDI`, `DELTA` and `ICEBERG` only. Parquet and Paimon exist in `xtable-core` as
   sources but are not wired into the service, so the UI must show them as unavailable rather than
   offer them.
2. **Sync mode is not a user choice.** `ConversionController` selects full or incremental from the
   target's persisted `TableSyncMetadata` and `ConversionSource.isIncrementalSyncSafeFrom`. The UI
   should report which mode was chosen, not offer a toggle the backend would ignore.
3. **Partition spec applies to Hudi sources only.** `ConversionService` reads `partition-spec` out
   of the request's `configurations` map and sets it as
   `HudiSourceConfig.PARTITION_FIELD_SPEC_CONFIG`. The field should appear for a Hudi source and be
   hidden otherwise.
4. **Nothing is persisted.** There is no run record, no history and no log retention. A page that
   answers "did last night's syncs work?" has nowhere to read that from today.

### What the API spec already says

`spec/rest-service-open-api.yaml` (version `0.0.1`) already specifies an asynchronous contract that
is **not implemented**:

- `POST /v1/conversion/table` accepts a `Prefer: respond-async` header and may answer `202` with a
  `SubmittedConversionResponse` carrying a `conversion-id`.
- `GET /v1/conversion/table/{conversion-id}` (`operationId: getConversionStatus`) returns `200`
  with the final `ConvertTableResponse`, or `202` while the conversion is still running.

Grepping `xtable-service/src/main/java` for `Prefer`, `respond-async` or `conversion-id` returns
nothing. The contract was written down and never built.

This matters for the design: the async layer the UI needs is largely **already specified**, and
this RFC proposes implementing that contract rather than inventing a parallel `/jobs` API.

### Deployment posture

`xtable-service/README.md` is explicit that the service "is a developer utility for local testing
and for trusted internal deployments", that it "ships with no authentication and no authorization",
and that "any client that can reach the port can start a conversion against any storage path the
service credentials can read or write".

Adding a browser UI does not change that, but it does change how likely someone is to expose the
port, because a UI invites being opened from another machine. This is addressed under
[Security](#security) and is the item most in need of community input.

## Implementation

### Where the UI lives

The UI is served from `xtable-service`, on Quarkus, as static assets under
`src/main/resources/META-INF/resources/ui/`.

The February 2025 comments on #497 settled on Spring MVC with Bootstrap. That exchange predates
`xtable-service`, which landed in May 2025 on Quarkus with JAX-RS. Introducing Spring now would add
a second web framework, a second dependency-injection container and a second HTTP server to the
build, to serve endpoints that have to be added to the Quarkus service anyway. Keeping one server
and one build is the reason for the change of direction; it should be a deliberate community
decision rather than an inherited one.

### Asynchronous conversion

This is the core of the work. The existing blocking endpoint stays exactly as it is: it is a
reasonable API for scripts and it is already released.

Implement the contract that `rest-service-open-api.yaml` already declares:

| Method | Path | Behaviour |
| --- | --- | --- |
| `POST` | `/v1/conversion/table` | Unchanged without the header. With `Prefer: respond-async`, returns `202` and a `conversion-id`. |
| `GET` | `/v1/conversion/table/{conversion-id}` | `202` while running, `200` with the `ConvertTableResponse` when finished. |

Both wrap the same `ConversionService.convertTable` call rather than duplicating conversion logic.

Three pieces are new and are **not** yet in the spec, so this RFC proposes them:

| Method | Path | Purpose | Screen |
| --- | --- | --- | --- |
| `GET` | `/v1/conversion/runs` | List recent runs with status, formats, mode and duration | Conversions list |
| `GET` | `/v1/conversion/runs/{conversion-id}/events?after=N` | Progress events newer than a cursor | Run detail |
| `POST` | `/v1/conversion/validate` | Probe a source path and report format, partitioning and reachability before submitting | New conversion |

```
                  POST /v1/conversion/table (Prefer: respond-async)
   browser  ──────────────────────────────────────────────▶  ConversionResource
      │                                                             │
      │     GET /v1/conversion/table/{id}    ┌────────────┐         │ submit
      │◀─────────────────────────────────────│  run store │◀────────┘
      │                                      │  (bounded) │
      │     GET /v1/conversion/runs          └────────────┘
      │◀────────────────────────────────────────────┘
      │                                      ┌──────────────────────────┐
      │     GET  .../runs/{id}/events?after= │ worker:                  │
      │◀─────────────────────────────────────│ ConversionService        │
                                             │   -> ConversionController│
                                             └──────────────────────────┘
```

### Run store

A run record holds the request, a status (`RUNNING`, `SUCCEEDED`, `FAILED`), start and end
timestamps, the sync mode reported per target, and either the `ConvertTableResponse` or the error.

This RFC proposes an **in-memory, bounded** store for the first increment: a fixed-capacity map
that evicts oldest-first, with the capacity exposed as a Quarkus config property alongside the
existing `xtable.hadoop-config-path`. History is lost on restart.

That is a deliberate limitation, not an oversight. Durable run history means choosing a datastore
and owning a schema, which is a larger decision than #497 needs in order to be useful, and one the
community should take separately if run history proves worth persisting.

### Progress events

An earlier draft of this RFC proposed tailing application logs per run through a Log4j2 appender,
streamed over SSE. Implementing it surfaced a problem: Quarkus routes logging through the JBoss
LogManager (`quarkus.log.level` in `application.properties`) while `xtable-core` logs through
Log4j2 via Lombok's `@Log4j2`. Capturing that output per run needs a logging bridge that
`xtable-service` does not declare, which is a dependency decision in its own right.

The service therefore records **its own progress events** instead: accepted, started, one per
target written, and succeeded or failed with the error. These are structured, framework
independent, and give the UI the progress trail issue #497 asks for without settling the logging
question first.

Each run holds a bounded list of events that evicts oldest-first, dropped with the run when it
leaves the store. Bounding matters: a full snapshot sync of a large table can emit many events, and
an unbounded per-run list behind a UI is a memory leak.

Delivery is by polling `?after=<sequence>` rather than SSE. Polling needs no new dependency, works
with the existing blocking JAX-RS setup, and is trivial to test. SSE over Mutiny `Multi` remains
the better end state and should be revisited once the logging and dependency questions above are
settled; the cursor-based endpoint shape does not have to change when it is.

### Catalog sync

The visual proposal includes a catalog sync screen. `RunCatalogSync` currently lives in
`xtable-utilities` as a `public static void main` CLI with no service-layer entry point, so
exposing it means either moving that logic into a form `xtable-service` can call, or duplicating
it.

This RFC **defers catalog sync**. It is a larger change than the conversion UI, it touches a
different module, and RFC-1 already owns the catalog-sync design. It should be its own follow-up
once the conversion UI exists.

### Frontend

Plain static HTML, CSS and JavaScript, served by Quarkus. **No npm or Node build step in the Maven
build.**

The reason is release mechanics rather than taste. An ASF source release has to be buildable from
the source tree, and every bundled third-party asset needs a license entry. The repo already
carries that machinery for Java dependencies (`release/scripts/validate_shaded_license_coverage.sh`,
`generate_shaded_license_metadata.py`, `validate_bundled_license_texts.py`) and a
`apache-rat-plugin` configuration with an explicit exclude list. Introducing a JavaScript dependency
tree would mean extending all of that to a second ecosystem for a developer utility.

Any vendored CSS or JS must be Apache-compatible, checked in with its license, added to the bundled
license metadata, and covered by the RAT excludes. Note the current excludes cover `**/*.json` and
`**/website/**` but nothing for a `ui/` asset directory, so that configuration needs a small
addition.

### Security

The UI inherits the service's posture: no authentication, no authorization. Concretely this RFC
proposes:

- Bind to loopback by default and say so in `xtable-service/README.md` next to the existing warning.
- Serve the UI only when explicitly enabled, via a config property that is off by default, so that
  an existing deployment of the service does not silently gain a browser interface on upgrade.
- Do not add an authentication mechanism in this RFC. Bolting one on is a real design decision with
  its own trade-offs and should not ride along with a UI change.

**Open question for reviewers.** Is an unauthenticated UI acceptable for a tool documented as a
local developer utility, or should the UI be gated behind authentication before it ships at all?
This is the question I would most like answered on the `dev@` thread.

### Phasing

| Phase | Contents | Depends on |
| --- | --- | --- |
| 1 | `Prefer: respond-async` + `GET /v1/conversion/table/{id}`, run store, run list endpoint | Nothing |
| 2 | Static UI: conversions list, new conversion form, run detail without logs | Phase 1 |
| 3 | Progress events and the bounded per-run event list | Phase 1 |
| 4 | Validation endpoint and the table detail screen | Phase 1 |
| 5 | Catalog sync | Separate RFC |

Phase 1 is useful on its own even with no UI, because it completes a contract the published API
spec already advertises.

## Rollout/Adoption Plan

- **Are there any breaking changes as part of this new feature/functionality?**
  - No. `POST /v1/conversion/table` without the `Prefer` header behaves exactly as it does today.
    All new endpoints are additive.
- **What impact (if any) will there be on existing users?**
  - None by default. The UI is off unless enabled by config. Existing script and API clients are
    unaffected.
- **If we are changing behavior how will we phase out the older behavior? When will we remove the
  existing behavior?**
  - Nothing is being phased out. The synchronous endpoint is the right API for scripts and stays.
- **If we need special migration tools, describe them here.**
  - None.

Because run history is in memory and bounded, users should be told plainly in the README that the
conversions list is a view of the current process, not an audit log.

## Test Plan

- Unit tests in `xtable-service` for the run store: eviction at capacity, status transitions, and
  the error path where `ConversionController.sync` throws.
- Resource tests alongside the existing `TestConversionResource` covering the async contract: `202`
  plus a `conversion-id` when `Prefer: respond-async` is sent, `202` from `getConversionStatus`
  while running, `200` with the payload once finished, and `404` for an unknown id.
- A regression test asserting the synchronous path is unchanged when the header is absent, since
  that is the released behaviour.
- A test that progress events are recorded in order, that the `after` cursor returns only newer
  events, and that the per-run event list evicts oldest-first at its cap.
- A test that a worker failure arriving as an `Error` rather than an `Exception` still moves the
  run to a terminal state. A missing engine on the classpath surfaces as
  `ServiceConfigurationError`, and a worker that only caught `Exception` left the run at
  `RUNNING` forever.
- Extend `ITConversionService` to run one conversion through the async path end to end and assert
  the result matches the synchronous path for the same input.
- The UI itself is static assets with no build step, so it is covered by manual verification
  against a locally running service rather than by browser automation. If reviewers want automated
  UI coverage, that is worth deciding now, because it would add a test-time browser dependency.
