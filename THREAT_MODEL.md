<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Apache XTable (Incubating) Threat Model

Status: v1 draft, pending PPMC review.
Applies to: `main` (0.5.0-SNAPSHOT) and all released versions.

This document states what the Apache XTable (Incubating) project treats as a security
vulnerability, and what it does not. It exists so that reporters, users and the PPMC
work from the same understanding of intended use before a report is filed. The ASF calls
this a project security model; see
[Documenting your security model](https://cwiki.apache.org/confluence/spaces/SECURITY/pages/308153000/Documenting+your+security+model).

It is a triage contract, not a security audit and not a design document. If you believe
you have found a vulnerability, see [Reporting](#reporting) below. Please do not open a
public issue for suspected vulnerabilities.

## 1. What XTable is

XTable translates the metadata that describes a table -- schema, partitioning, file
listings, column statistics and snapshot history -- between table formats, without
rewriting or copying the underlying data files.

- **Sources:** Apache Hudi, Apache Iceberg, Delta Lake (through Delta Standalone or Delta
  Kernel), Apache Paimon, and plain Parquet datasets.
- **Targets:** Apache Hudi, Apache Iceberg, and Delta Lake (through Delta Standalone or
  Delta Kernel).
- **Catalogs:** XTable can read a source table through an Iceberg catalog, and can
  register converted tables in AWS Glue or a Hive Metastore.

XTable is a **batch tool run by an operator against storage that operator controls**. The
supported ways to run it are:

- the `xtable-utilities` bundle jar, through `RunSync` (table conversion) or
  `RunCatalogSync` (catalog synchronization), driven by YAML configuration files. The
  project does not publish this jar or a container image; operators build it from
  source, and the root `Dockerfile` is a build recipe, not a published artifact,
- as a library on the JVM (`xtable-api`, `xtable-core` and the catalog modules
  `xtable-aws` and `xtable-hive-metastore`).

`RunSync` can loop in continuous mode, but it opens no network port. XTable is not a
network service and not a multi-tenant system. It runs with the storage and catalog
credentials the operator gives it, and acts on the paths the operator configures.

The repository also contains `xtable-service`, a Quarkus HTTP wrapper around the
conversion API. It is a developer utility, not a supported way to run XTable. See
section 4.1.

## 2. Trust boundaries

**Trusted inputs.** The dataset configuration, the Hadoop configuration, the Iceberg
catalog configuration, the conversion and catalog configuration, CLI arguments,
environment variables and credentials, the catalog endpoints named in configuration, the
local filesystem, the JVM classpath, and the identity of the process. Whoever supplies
these is the operator, and is trusted completely. XTable does not attempt to defend
against the person who invoked it.

**Untrusted input, in scope.** The *bytes* of table metadata and data files that XTable
reads from storage. This covers the source table, and the existing target table that
XTable reads back during an incremental sync. An operator may legitimately convert a
table written by someone else, so crafted table content that escapes the conversion
boundary is a real concern.

**Not a trust boundary.** The `xtable-service` HTTP port. There is no security boundary
at that port and none is claimed. See section 4.1.

## 3. In scope

The following are treated as valid vulnerabilities:

1. **Escape from the conversion boundary via crafted table content.** Table metadata,
   manifests, logs or data file footers that cause XTable to:
   - write or delete anything outside the configured target table's metadata location,
   - execute attacker-controlled code, for example through unsafe deserialization,
     polymorphic type handling, XXE, or archive path traversal, or
   - write to a table, catalog or catalog entry other than the configured one.
2. **Credential disclosure.** Storage or catalog credentials written into logs, exception
   messages, converted table metadata, catalog entries, or any other output.
3. **Writing to an unintended target.** Conversion or catalog sync that writes to a table,
   path or catalog other than the one configured, for reasons other than crafted input.
4. **Vulnerable dependencies reachable in documented use**, including dependencies shaded
   into the published jars.
5. **Integrity of published release artifacts** -- signatures, checksums, and the contents
   of the source release and of the artifacts on Maven Central.

## 4. Out of scope (explicit non-goals)

The following are **not** treated as vulnerabilities in XTable:

1. **`xtable-service` reachable from an untrusted network.** `xtable-service` is a
   developer utility for exercising the conversion API locally. It has no authentication
   or authorization layer. Its README states this deployment scope. It is not described in
   the user documentation at https://xtable.apache.org, there is no accepted design making
   it a deployable service, and the project publishes no container image, hardened
   configuration, or deployment guidance for it. Exposing it on a network reachable by
   untrusted parties is unsupported. Any client that can reach the port can start a
   conversion against any path the service credentials can read or write. If you choose
   to run it beyond loopback, placing authentication in front of it is your
   responsibility.
2. **Any finding that requires the operator to deliberately expose a credential-loaded
   XTable process or port.** XTable holds whatever privileges the operator grants it;
   exposing that process is outside its intended use.
3. **Class names in configuration.** Configuration may name Java classes that XTable
   loads by reflection, for example a conversion source provider, a partition value
   extractor, an Iceberg catalog implementation, or an AWS client or credentials provider.
   Loading the class the operator names is intended behaviour.
4. **Data file references in source metadata.** Table formats allow a table to reference
   data files outside its base path, for example an Iceberg table with `write.data.path`
   set, or with files added by `add_files`. XTable carries these references into the
   target as absolute paths, and may open a referenced Parquet file to read its footer
   statistics. XTable does not check that a referenced file belongs to the table. A
   source table can therefore name any file the XTable credentials can read, and that
   file's path, row count and column statistics can appear in the target metadata. Use
   credentials scoped to the tables you convert; see section 6.
5. **The `demo/` directory and docker playground assets.** These are development fixtures.
   Default or absent credentials in them are expected.
6. **The operator's own IAM, storage and catalog permissions.** Over-broad credentials
   granted to the XTable process are a deployment concern, not an XTable defect.
7. **XTable acting on paths the operator configured.** XTable writes where it is told,
   using the identity it is given. That is the entire function of the tool.
8. **Resource exhaustion from large or pathological tables.** XTable does not bound the
   work implied by its input.
9. **Vulnerabilities in Apache Hudi, Apache Iceberg, Delta Lake, Apache Paimon, Apache
   Parquet, Apache Spark, Apache Hadoop, the AWS SDK, Quarkus or other upstream
   projects.** Please report these to the responsible project. We will help route a
   report if it is unclear where it belongs. A flaw in upstream code that XTable reaches
   only through documented use is in scope under section 3.4.
10. **Findings that presuppose the attacker already has code execution or filesystem
    access as the XTable process user, or can modify the JVM classpath.**

## 5. Security properties XTable does NOT provide

Stated plainly, so they are not mistaken for defects:

- No authentication or authorization on `xtable-service`.
- No sandboxing or isolation of table content beyond ordinary parsing.
- No check that a data file referenced by table metadata belongs to that table.
- No isolation between a caller and the credentials the XTable process holds.
- No multi-tenancy. One XTable process serves one operator's intent.
- No bound on the time, memory or I/O a conversion may consume.

## 6. Operator responsibilities

- Run `xtable-service` on loopback, or only within a network you control, and put your own
  authentication in front of it if you expose it at all. In Quarkus production mode the
  HTTP server listens on all interfaces unless `quarkus.http.host` is set.
- Grant the XTable process only the storage and catalog permissions its conversions need.
  Do not give it read access to data that the source table's writers must not see.
- Convert tables from storage you trust, or run the conversion in an isolated environment.
- Keep XTable current, and monitor upstream advisories for the table formats and catalogs
  you use.

## 7. Known non-findings

Reports matching these have been considered and are dispositioned as noted:

- **`POST /v1/conversion/table` on `xtable-service` requires no authentication.**
  `KNOWN-NON-FINDING`, out of model per section 4.1. Reported privately in July 2026 and
  assessed by the PPMC.
- **`xtable-service` listens on all interfaces by default.** `VALID-HARDENING`. Not a
  vulnerability in a developer utility, but a loopback default is welcome as an ordinary
  pull request.
- **`spec/rest-service-open-api.yaml` documents a `403` response that the implementation
  never returns.** `VALID-HARDENING`. A contract defect, handled as an ordinary bug, not a
  vulnerability.
- **XTable writes converted metadata to a path supplied by the caller.** `BY-DESIGN`, per
  sections 2 and 4.7.
- **XTable instantiates a class named in configuration.** `BY-DESIGN`, per section 4.3.
- **A converted table references, or carries statistics from, a file outside the source
  table's base path.** `BY-DESIGN`, per section 4.4.
- **Demo fixtures contain default credentials.** `KNOWN-NON-FINDING`, per section 4.5.

## 8. Triage dispositions

The PPMC classifies reports using these labels:

- `VALID` -- a vulnerability against a property in section 3. Handled under the ASF
  security process, with a CVE where warranted.
- `VALID-HARDENING` -- a real defect worth fixing that does not breach a claimed security
  property. Fixed in the open as an ordinary issue, no CVE, no embargo.
- `OUT-OF-MODEL` -- accurate observation, outside the scope in section 4.
- `BY-DESIGN` -- the described behaviour is the tool's intended function.
- `KNOWN-NON-FINDING` -- previously considered and recorded in section 7.

## 9. Conditions that would change this model

Section 4.1 depends on `xtable-service` remaining a developer utility, and section 1
depends on XTable having no supported network surface. This model must be revised if any
of the following happens:

- the REST service RFC (pull request #702) is accepted and `xtable-service` becomes a
  supported deployment target,
- the web UI and asynchronous conversion API for `xtable-service` (pull requests #945 and
  #946) are merged,
- a container image, hosted distribution or production deployment guide for
  `xtable-service` is published,
- `xtable-service` is documented on https://xtable.apache.org as a deployable service,
- `xtable-service` gains an authentication layer, at which point its correctness becomes a
  claimed security property,
- XTable ships a network-facing agent interface, such as the MCP surface proposed in
  issue #891, or
- a new supported way to run XTable is added, such as the `xtable-spark-runtime` bundle
  proposed in pull request #964.

Anyone proposing such a change should update this document in the same pull request.

## Reporting

Report suspected vulnerabilities privately to security@apache.org, following the
[ASF vulnerability reporting process](https://www.apache.org/security/). Do not open a
public GitHub issue. Reports covered by section 4 or section 7 may be answered with a
pointer to this document.

For defects that are not security-sensitive, including anything listed in section 7,
please open a normal issue at https://github.com/apache/incubator-xtable/issues.
