#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -o errexit
set -o nounset
set -o pipefail

# Asserts that every jar we publish carries its licensing information in exactly
# one canonical place: META-INF/LICENSE, META-INF/NOTICE and, for the shaded
# bundles, the third party texts under META-INF/licenses/.
#
# This exists because the layout regressed silently across several releases
# (XTABLE-701): every jar shipped a second, differently named copy of the
# licensing information as META-INF/LICENSE-bundled and META-INF/NOTICE-bundled,
# and the shaded jars additionally carried each bundled dependency's own license
# file in whatever location that dependency happened to use, which made the
# licensing of a bundle very hard to review.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${ROOT_DIR}"

# Entries a jar is allowed to carry. Anything else that looks like licensing
# information is a finding: it means the information is duplicated, or filed
# somewhere a reviewer will not think to look.
is_allowed_entry() {
  case "$1" in
    META-INF/LICENSE|META-INF/NOTICE|META-INF/DISCLAIMER|META-INF/DISCLAIMER-WIP|META-INF/DEPENDENCIES)
      return 0
      ;;
    META-INF/licenses/*)
      return 0
      ;;
  esac
  return 1
}

# xtable-utilities is not a release artifact: maven-deploy-plugin is skipped for
# it, and create_source_release.sh leaves it out of the source tarball. Its shaded
# jar has no META-INF/LICENSE at all, which is worth fixing on its own but is not
# part of the published set that XTABLE-701 is about. validate_shaded_license_
# coverage.sh skips it for the same reason.
SKIPPED_MODULES=(
  "xtable-utilities"
)

discover_jars() {
  local skip_pattern
  skip_pattern="$(printf '%s\n' "${SKIPPED_MODULES[@]}" | paste -sd'|' -)"
  find . -type f -name '*.jar' 2>/dev/null \
    | grep -E '/target/[^/]+\.jar$' \
    | grep -vE '\-(sources|javadoc)\.jar$' \
    | grep -vE '/target/original-' \
    | grep -vE "^\./(${skip_pattern})/" \
    | sort
}

# Only the shaded modules, never the whole reactor: xtable-service needs a newer
# JDK than the one this runs under in CI, and it produces no shaded jar anyway.
shade_modules() {
  grep -rl '<artifactId>maven-shade-plugin</artifactId>' --include=pom.xml . \
    | xargs -n1 dirname \
    | sed 's|^\./||' \
    | grep -v '^\.$' \
    | sort -u \
    | paste -sd, -
}

jars=("$@")

if [[ ${#jars[@]} -eq 0 ]]; then
  while IFS= read -r jar; do
    jars+=("${jar}")
  done < <(discover_jars)
fi

if [[ ${#jars[@]} -eq 0 ]]; then
  modules="$(shade_modules)"
  echo "No jars found under */target, building the shaded modules first: ${modules}"
  ./mvnw -q package -B -DskipTests -Dmaven.build.cache.enabled=false -pl "${modules}" -am
  while IFS= read -r jar; do
    jars+=("${jar}")
  done < <(discover_jars)
fi

if [[ ${#jars[@]} -eq 0 ]]; then
  echo "No jars found under */target even after building. Pass jar paths as arguments."
  exit 1
fi

overall_status=0

for jar in "${jars[@]}"; do
  if [[ ! -f "${jar}" ]]; then
    echo "FAIL ${jar}: not a file."
    overall_status=1
    continue
  fi

  # original-*.jar is the pre-shade copy the shade plugin leaves behind, it is
  # never published.
  case "$(basename "${jar}")" in
    original-*) continue ;;
  esac

  entries="$(unzip -Z1 "${jar}" | grep -v '/$' || true)"

  # Only consider entries that look like licensing information. Class files that
  # merely contain the word "license" in their name are not interesting here.
  # Deliberately matches the words anywhere in the path, not just at a path
  # boundary: dependencies ship things like META-INF/ASM_LICENSE.txt and
  # META-INF/CLI-LICENSE.txt, which a boundary-anchored pattern would miss.
  candidates="$(printf '%s\n' "${entries}" \
    | grep -vE '\.class$' \
    | grep -iE '(licen[cs]e|notice|disclaimer|copying)|(^|/)about_files/' \
    || true)"

  findings=()
  while IFS= read -r entry; do
    [[ -z "${entry}" ]] && continue
    if ! is_allowed_entry "${entry}"; then
      findings+=("${entry}")
    fi
  done < <(printf '%s\n' "${candidates}")

  # Match in the shell rather than piping into "grep -q": grep exits on the first
  # match, the writer then takes SIGPIPE, and under "set -o pipefail" the whole
  # pipeline reports failure. That only shows up on the big shaded jars, where the
  # writer has not finished yet, so it would fail intermittently and only for the
  # bundles.
  if [[ $'\n'${entries}$'\n' != *$'\n'"META-INF/LICENSE"$'\n'* ]]; then
    echo "FAIL $(basename "${jar}"): META-INF/LICENSE is missing."
    overall_status=1
    continue
  fi

  if [[ ${#findings[@]} -gt 0 ]]; then
    echo "FAIL $(basename "${jar}"): licensing information outside the canonical locations:"
    printf '  - %s\n' "${findings[@]}"
    overall_status=1
  else
    texts="$(printf '%s\n' "${entries}" | grep -c '^META-INF/licenses/' || true)"
    echo "OK   $(basename "${jar}"): META-INF/LICENSE, META-INF/NOTICE, ${texts} bundled license text(s)."
  fi
done

exit "${overall_status}"
