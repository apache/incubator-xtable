<!--
 - Licensed to the Apache Software Foundation (ASF) under one
 - or more contributor license agreements.  See the NOTICE file
 - distributed with this work for additional information
 - regarding copyright ownership.  The ASF licenses this file
 - to you under the Apache License, Version 2.0 (the
 - "License"); you may not use this file except in compliance
 - with the License.  You may obtain a copy of the License at
 -
 -     http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
-->

# Apache XTable Agent Guide

This file gives repository-specific instructions for agents working in this repo.

## Scope

- Applies to the whole repository unless a deeper `AGENTS.md` is added later.

## Repo Structure

- `xtable-api`: shared public interfaces and SPI contracts.
- `xtable-core`: core conversion logic, sync flow, and common implementation code.
- `xtable-hudi-support`: Hudi-specific support modules, including shaded extensions under `xtable-hudi-support-extensions`.
- `xtable-utilities`: bundled CLI and shaded distribution jar.
- `xtable-aws`: AWS-related shaded support dependencies.
- `xtable-hive-metastore`: Hive Metastore shaded support dependencies.
- `xtable-service`: service-layer code.
- `release/scripts`: release and compliance automation, including shaded license tooling.
- `spec` and `rfc`: design docs and proposal material.
- `website`: project site content.

When changing code, prefer to work in the narrowest module that owns the behavior. If a change crosses module boundaries, verify all affected modules instead of only the top-level caller.

## Common Validation

Use Java 11 for local Maven work.

Common commands:

```bash
./mvnw test
./mvnw verify
./mvnw spotless:check
./mvnw spotless:apply
```

Prefer targeted commands while iterating:

```bash
./mvnw -pl <module[,module...]> test
./mvnw -pl <module[,module...]> verify
./mvnw -pl <module[,module...]> -Dtest=<TestClass> test
```

Test control flags wired in the root `pom.xml`:

- `-DskipTests` skips both unit and integration tests.
- `-DskipUTs` skips surefire unit tests only.
- `-DskipITs` skips failsafe integration tests only.

Validation expectations:

- For a focused code change, run the narrowest module test or verify command that covers the edited behavior.
- For shared build logic, parent POM changes, cross-module APIs, or release tooling, run broader Maven validation before finishing.
- If formatting might be affected, run `./mvnw spotless:check` and use `./mvnw spotless:apply` if needed.

## Dependency Changes

When adding, removing, or upgrading a dependency, always follow this sequence:

1. Update the relevant `pom.xml` dependency declarations.
2. If the module uses `maven-shade-plugin`, regenerate the runtime dependency tree and keep the shade `<artifactSet><includes>` list aligned to runtime dependencies only.
3. Regenerate bundled license metadata for shaded modules.
4. Run the shaded license validator.
5. Run the narrowest Maven verification needed for the changed modules.

Do not stop after updating the Maven dependency declaration alone.

## Shaded Modules

Modules with `maven-shade-plugin` must use explicit `<artifactSet><includes>` entries.

Rules:

- Includes must reflect the current `dependency:tree -Dscope=runtime` output.
- Do not include `provided` or `test` dependencies.
- Do not hand-wave transitive dependencies; if they are shaded, they must be listed explicitly.
- If a dependency upgrade changes the runtime tree, update the include list to match.

Current shaded modules include:

- `xtable-aws`
- `xtable-hive-metastore`
- `xtable-hudi-support/xtable-hudi-support-extensions`
- `xtable-utilities`

## Required Commands For Dependency Work

Generate runtime dependency trees for changed shaded modules:

```bash
./mvnw -pl <module[,module...]> -am -DskipTests dependency:tree -Dscope=runtime -DoutputType=text -DoutputFile=target/dependency-tree-runtime.txt
```

Regenerate bundled license metadata:

```bash
python3 release/scripts/generate_shaded_license_metadata.py
```

Validate shaded dependency license coverage and ASF license-family compliance:

```bash
release/scripts/validate_shaded_license_coverage.sh
```

If only one or two modules changed, prefer targeted Maven verification:

```bash
./mvnw -pl <module[,module...]> -am -DskipTests dependency:tree -Dscope=runtime -DoutputType=text -DoutputFile=target/dependency-tree-runtime.txt
```

If broader confidence is needed, run:

```bash
./mvnw clean install -ntp -B
```

## Bundled License Metadata

Shaded modules must keep these files current:

- `src/main/resources/META-INF/LICENSE-bundled`
- `src/main/resources/META-INF/NOTICE-bundled`

Do not manually rewrite large dependency/license sections unless necessary.
Prefer regenerating with:

```bash
python3 release/scripts/generate_shaded_license_metadata.py
```

Notes:

- The generator uses runtime dependency trees plus a small override table for old artifacts with incomplete Maven license metadata.
- Category B licenses can be present in convenience binaries, but the validator will warn so they stay visible.
- Missing bundled metadata is treated as a failure.

## GitHub Actions

Pull requests are expected to pass:

- `.github/workflows/mvn-ci-build.yml`
- `.github/workflows/mvn-license-check.yml`

The license workflow runs:

- `./mvnw apache-rat:check -B`
- `release/scripts/validate_shaded_license_coverage.sh`

If you change dependency behavior, assume this workflow must still pass.

For non-dependency changes, expect the main Maven CI workflow to be the baseline bar. If you touch release scripts or contributor automation, also sanity-check the affected scripts locally when practical.

## Editing Guidance

- Keep generated dependency/license sections deterministic and sorted where the repo already expects that.
- Prefer targeted edits and targeted Maven runs before broader builds.
- Do not remove bundled metadata files from shaded modules.
- If you add a new shaded module, update this guide and ensure the generator/validator cover it.
- Preserve ASF license headers in new source files and scripts when they are required by surrounding repo conventions.
- Avoid unrelated formatting churn in large generated or metadata-heavy files.
- Check for module-local resources under `src/main/resources` and `src/test/resources` when behavior depends on bundled configs or sample files.

## Non-Dependency Change Checklist

- Identify the owning module and keep the change as narrow as practical.
- Run targeted tests or `verify` for the affected module set.
- Run `spotless:check` if Java formatting may have changed.
- If you changed release tooling or CI-facing scripts, run those scripts directly when possible.
- If you changed shaded modules incidentally, make sure bundled license metadata still matches the current shaded dependencies.

## Final Checklist For Dependency PRs

- Dependency declarations updated.
- Runtime tree regenerated for changed shaded modules.
- Shade include lists match runtime dependencies only.
- `python3 release/scripts/generate_shaded_license_metadata.py` run.
- `release/scripts/validate_shaded_license_coverage.sh` run successfully.
- Any expected Category B warnings reviewed.
- Relevant Maven verification command run successfully.
