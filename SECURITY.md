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

# Security Policy

## Reporting a vulnerability

Please do not report security vulnerabilities through public GitHub issues, pull
requests, or mailing lists.

Report suspected vulnerabilities privately to security@apache.org, following the
[ASF vulnerability reporting process](https://www.apache.org/security/). The ASF security
team will forward the report to the Apache XTable (Incubating) PPMC, who will
acknowledge it, assess it, and coordinate a fix and disclosure.

A useful report includes:

- the XTable version or commit,
- how XTable was run (`RunSync`, `RunCatalogSync`, or as a library) and the source and
  target formats involved,
- the steps or crafted input needed to reproduce the issue, and
- the security property from [THREAT_MODEL.md](THREAT_MODEL.md) that the issue breaks.

## What counts as a vulnerability

[THREAT_MODEL.md](THREAT_MODEL.md) states what the project treats as a vulnerability,
what it treats as out of scope, and how the PPMC classifies reports. Please read it
before filing. Reports that match its out-of-scope or known non-finding sections may be
answered with a pointer to that document.

In particular, `xtable-service` is a developer utility with no authentication, and
exposing it to an untrusted network is not a supported use.

## Supported versions

Security fixes are made on `main` and ship in the next release. The latest release is
listed at https://xtable.apache.org/releases/downloads/. Whether a fix is also backported
to an earlier release line is decided by the PPMC for each issue.

## Other issues

For defects that are not security-sensitive, open an issue at
https://github.com/apache/incubator-xtable/issues.
