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
3. Keep the module's `xtable.bundled.artifactIds` property in step with those includes; it decides which artifacts the bundled LICENSE lists.
4. If a newly bundled artifact is not Apache-2.0, add its license text under `src/main/resources/META-INF/licenses/`. If its POM declares no license, add it to `release/license/bundled/supplemental-models.xml`.
5. Build the shaded modules and run the bundled license validator.
6. Run the narrowest Maven verification needed for the changed modules.

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
- `xtable-spark-runtime`
- `xtable-utilities`

`xtable-utilities` is not built by the License Check while it is not published. Keep its license texts useful for local builds, but do not treat it as release-blocking until that artifact is published.

## Required Commands For Dependency Work

Generate runtime dependency trees for changed shaded modules:

```bash
./mvnw -pl <module[,module...]> -am -DskipTests dependency:tree -Dscope=runtime -DoutputType=text -DoutputFile=target/dependency-tree-runtime.txt
```

Build the shaded bundles and check that every bundled non-Apache-2.0 dependency ships its license text:

```bash
./mvnw -B -DskipTests -pl xtable-aws,xtable-hive-metastore,xtable-hudi-support/xtable-hudi-support-extensions,xtable-spark-runtime -am package dependency:tree -DoutputType=text -DoutputFile=target/dependency-tree.txt
python3 release/scripts/validate_bundled_license_texts.py
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

Nothing here is maintained by hand except the license texts and the artifact list:

- `META-INF/LICENSE` is rendered at build time by `maven-remote-resources-plugin` from `release/license/bundled/META-INF/LICENSE.vm`. It carries the Apache License 2.0 and lists only the bundled non-Apache-2.0 components, each with a pointer to its text under `META-INF/licenses/`. Bundled Apache-2.0 dependencies need no entry.
- `META-INF/NOTICE` is merged by the shade plugin's `ApacheNoticeResourceTransformer` from the bundled artifacts' own NOTICE files.
- A module opts in with `xtable.bundled.resources.skip=false` and lists its bundled artifact IDs in `xtable.bundled.artifactIds`.
- `release/license/bundled/supplemental-models.xml` supplies a license for artifacts whose POM declares none, and pins the license taken for a dual-licensed artifact.

A rendered LICENSE line starting `UNMAPPED-LICENSE:` names a bundled artifact whose POM declares no license; give it one in `supplemental-models.xml`. A new spelling of a known license is normalised in the `canonical` map in `LICENSE.vm`.

## GitHub Actions

Pull requests are expected to pass:

- `.github/workflows/mvn-ci-build.yml`
- `.github/workflows/mvn-license-check.yml`

The license workflow runs:

- `./mvnw apache-rat:check -B`
- a package build of the shaded modules
- `python3 release/scripts/validate_bundled_license_texts.py`

If you change dependency behavior, assume this workflow must still pass.

For non-dependency changes, expect the main Maven CI workflow to be the baseline bar. If you touch release scripts or contributor automation, also sanity-check the affected scripts locally when practical.

## Editing Guidance

- Keep generated dependency/license sections deterministic and sorted where the repo already expects that.
- Prefer targeted edits and targeted Maven runs before broader builds.
- Do not remove license texts under `META-INF/licenses/` while the artifact they cover is still bundled.
- If you add a new shaded module, set `xtable.bundled.resources.skip` and `xtable.bundled.artifactIds` in its POM, use the LICENSE and NOTICE transformers the other shaded modules use, add it to the License Check build, and update this guide.
- Preserve ASF license headers in new source files and scripts when they are required by surrounding repo conventions.
- Avoid unrelated formatting churn in large generated or metadata-heavy files.
- Check for module-local resources under `src/main/resources` and `src/test/resources` when behavior depends on bundled configs or sample files.

## Non-Dependency Change Checklist

- Identify the owning module and keep the change as narrow as practical.
- Run targeted tests or `verify` for the affected module set.
- Run `spotless:check` if Java formatting may have changed.
- If you changed release tooling or CI-facing scripts, run those scripts directly when possible.
- If you changed shaded modules incidentally, make sure `xtable.bundled.artifactIds` and the license texts still match the current shaded dependencies.

## Final Checklist For Dependency PRs

- Dependency declarations updated.
- Runtime tree regenerated for changed shaded modules.
- Shade include lists match runtime dependencies only.
- `xtable.bundled.artifactIds` updated to match the includes.
- A license text added for each newly bundled non-Apache-2.0 dependency.
- `python3 release/scripts/validate_bundled_license_texts.py` run successfully on freshly built bundles.
- Any expected Category B warnings reviewed.
- Relevant Maven verification command run successfully.
