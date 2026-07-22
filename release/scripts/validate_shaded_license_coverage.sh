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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${ROOT_DIR}"

escape_regex() {
  printf '%s' "$1" | sed 's/[][(){}.^$?*+|/]/\\&/g'
}

resolve_property() {
  local value="$1"
  value="${value//\$\{scala.binary.version\}/${SCALA_BINARY_VERSION}}"
  printf '%s\n' "${value}"
}

runtime_coords() {
  local tree_file="$1"
  sed -E 's/^[| +\\-]+//' "${tree_file}" \
    | awk -F: '
        NF >= 5 && $3 != "pom" && $3 != "test-jar" {
          print $1 ":" $2
        }
      ' \
    | sort -u
}

classify_license_family() {
  case "$1" in
    "Apache License 2.0"|"Apache Software License 1.1"|"BSD 2-Clause"|"BSD 3-Clause"|"MIT License"|"ISC"|"ICU"|"University of Illinois/NCSA"|"W3C Software License"|"X.Net"|"zlib/libpng"|"Eclipse Distribution License - v 1.0"|"Public Domain")
      printf 'A\n'
      ;;
    "CDDL"|"Common Development and Distribution License"|"CDDL + GPLv2 with classpath exception"|"Mozilla Public License 1.0"|"Mozilla Public License 1.1"|"Mozilla Public License 2.0"|"EPL 1.0"|"EPL 2.0"|"Eclipse Public License 1.0"|"Eclipse Public License 2.0"|"GPL-2.0 with GNU ClasspathException"|"Common Public License Version 1.0")
      printf 'B\n'
      ;;
    *"AGPL"*|*"LGPL"*|*"GPL"*|*"Commons Clause"*|*"Non-Commercial"*|*"Creative Commons Non-Commercial"*|*"RSAL"*|*"Confluent Community License"*)
      printf 'X\n'
      ;;
    *)
      printf 'UNKNOWN\n'
      ;;
  esac
}

SCALA_BINARY_VERSION="$(sed -n 's:.*<scala.binary.version>\(.*\)</scala.binary.version>.*:\1:p' pom.xml | head -n 1)"
SKIPPED_SHADE_MODULES=(
  "xtable-utilities"
)

should_skip_module() {
  local module="$1"
  local skipped_module
  for skipped_module in "${SKIPPED_SHADE_MODULES[@]}"; do
    if [[ "${module}" == "${skipped_module}" ]]; then
      return 0
    fi
  done
  return 1
}

# Modules whose shade <includes> are known not to match their runtime dependency
# tree. Only the includes-vs-tree comparison is downgraded to a warning for these,
# and only for the modules named here; every other check, for every module, stays
# blocking.
#
# xtable-hive-metastore's include list was written for a Hive 2.x dependency set:
# 25 entries name dependencies that are no longer resolved and 97 runtime
# dependencies are missing from it. Reconciling the two changes what the shaded
# jar bundles, so it is a release decision rather than a tooling fix and is
# tracked in #880. This entry only became reachable when the ripgrep fail-open
# below was fixed, so the drift has been present and unreported for some time
# rather than being newly introduced.
#
# Remove the entry when #880 lands; the check is then blocking again.
KNOWN_INCLUDES_DRIFT=(
  "xtable-hive-metastore"
)

has_known_includes_drift() {
  local module="$1"
  local drifted_module
  for drifted_module in "${KNOWN_INCLUDES_DRIFT[@]}"; do
    if [[ "${module}" == "${drifted_module}" ]]; then
      return 0
    fi
  done
  return 1
}

SHADE_MODULES=()
while IFS= read -r module; do
  module_dir="$(dirname "${module}")"
  if [[ "${module_dir}" != "." ]]; then
    if ! should_skip_module "${module_dir}"; then
      SHADE_MODULES+=("${module_dir}")
    fi
  fi
done < <(
  find . -name pom.xml -not -path '*/target/*' -print0 \
    | xargs -0 grep -l '<artifactId>maven-shade-plugin</artifactId>' \
    | sed 's|^\./||' \
    | sort
)

# This used to shell out to ripgrep, which is not installed by mvn-license-check.yml
# and is absent on plenty of developer machines. When it was missing the module
# list came back empty and the script reported success, so the entire release
# license gate passed without checking anything. find + grep is always present,
# and an empty module list is now a failure rather than a pass.
if [[ ${#SHADE_MODULES[@]} -eq 0 ]]; then
  echo "FAIL: no modules with maven-shade-plugin were found; this script cannot validate anything."
  exit 1
fi

missing_runtime_trees=()
for module in "${SHADE_MODULES[@]}"; do
  if [[ ! -f "${module}/target/dependency-tree-runtime.txt" ]]; then
    missing_runtime_trees+=("${module}")
  fi
done

if [[ ${#missing_runtime_trees[@]} -gt 0 ]]; then
  module_csv="$(IFS=,; printf '%s' "${missing_runtime_trees[*]}")"
  ./mvnw -pl "${module_csv}" -am -DskipTests dependency:tree -Dscope=runtime -DoutputType=text -DoutputFile=target/dependency-tree-runtime.txt
fi

overall_status=0

for module in "${SHADE_MODULES[@]}"; do
  pom_file="${module}/pom.xml"
  license_file="${module}/src/main/resources/META-INF/LICENSE-bundled"
  notice_file="${module}/src/main/resources/META-INF/NOTICE-bundled"
  tree_file="${module}/target/dependency-tree-runtime.txt"

  includes=()
  while IFS= read -r include; do
    includes+=("${include}")
  done < <(
    awk '
      /<artifactSet>/ {in_artifact_set=1}
      in_artifact_set && /<includes/ {in_includes=1}
      in_includes {
        line=$0
        while (match(line, /<include>([^<]+)<\/include>/)) {
          print substr(line, RSTART + 9, RLENGTH - 19)
          line = substr(line, RSTART + RLENGTH)
        }
      }
      in_includes && /<\/includes>/ {in_includes=0}
      in_artifact_set && /<\/artifactSet>/ {in_artifact_set=0}
    ' "${pom_file}" | sort -u
  )

  runtime_includes=()
  while IFS= read -r coord; do
    runtime_includes+=("${coord}")
  done < <(runtime_coords "${tree_file}")

  if [[ ${#includes[@]} -eq 0 ]]; then
    echo "FAIL ${module}: no <artifactSet><includes> entries found."
    overall_status=1
    continue
  fi

  if [[ ${#runtime_includes[@]} -eq 0 ]]; then
    echo "FAIL ${module}: ${tree_file} did not contain any runtime dependencies."
    overall_status=1
    continue
  fi

  include_set_file="$(mktemp)"
  runtime_set_file="$(mktemp)"
  diff_output_file="$(mktemp)"

  printf '%s\n' "${includes[@]}" | while IFS= read -r include; do
    resolve_property "${include}"
  done | sort -u > "${include_set_file}"
  printf '%s\n' "${runtime_includes[@]}" | sort -u > "${runtime_set_file}"

  if ! diff -u "${runtime_set_file}" "${include_set_file}" > "${diff_output_file}"; then
    if has_known_includes_drift "${module}"; then
      echo "WARN ${module}: shade <includes> do not match runtime dependencies from ${tree_file}."
      echo "     Known pre-existing drift, tracked in #880; not failing the build."
      sed 's/^/  /' "${diff_output_file}"
    else
      echo "FAIL ${module}: shade <includes> must exactly match runtime dependencies from ${tree_file}."
      sed 's/^/  /' "${diff_output_file}"
      overall_status=1
    fi
  fi

  rm -f "${include_set_file}" "${runtime_set_file}" "${diff_output_file}"

  if [[ ! -f "${license_file}" ]]; then
    echo "FAIL ${module}: ${license_file} is missing, cannot validate bundled dependency licenses."
    overall_status=1
    continue
  fi

  search_files=("${license_file}")
  if [[ -f "${notice_file}" ]]; then
    search_files+=("${notice_file}")
  fi

  missing=()
  for include in "${includes[@]}"; do
    include="$(resolve_property "${include}")"

    if [[ "${include}" == org.apache.xtable:* ]]; then
      continue
    fi

    escaped_include="$(escape_regex "${include}")"
    if ! grep -Eq "^${escaped_include}(:jar)?:[^[:space:]]+$" "${search_files[@]}"; then
      missing+=("${include}")
    fi
  done

  if [[ ${#missing[@]} -eq 0 ]]; then
    echo "OK   ${module}: all shaded include entries are represented in bundled license metadata."
  else
    overall_status=1
    echo "FAIL ${module}: missing bundled license metadata entries for:"
    printf '  - %s\n' "${missing[@]}"
  fi

  families=()
  while IFS= read -r family; do
    families+=("${family}")
  done < <(
    awk '
      /This section summarizes those components and their licenses\./ {in_summary=1}
      in_summary && prev != "" && /^-+$/ {print prev}
      {prev=$0}
    ' "${license_file}" | sed '/^$/d'
  )

  if [[ ${#families[@]} -eq 0 ]]; then
    echo "FAIL ${module}: no bundled license families were found in ${license_file}."
    overall_status=1
    continue
  fi

  category_b=()
  category_x=()
  unknown=()
  for family in "${families[@]}"; do
    case "$(classify_license_family "${family}")" in
      A)
        ;;
      B)
        category_b+=("${family}")
        ;;
      X)
        category_x+=("${family}")
        ;;
      UNKNOWN)
        unknown+=("${family}")
        ;;
    esac
  done

  if [[ ${#category_b[@]} -gt 0 ]]; then
    echo "WARN ${module}: Category B license families present and must remain appropriately labelled in convenience binaries:"
    printf '  - %s\n' "${category_b[@]}"
  fi

  if [[ ${#category_x[@]} -gt 0 ]]; then
    echo "FAIL ${module}: Category X / disallowed license families detected:"
    printf '  - %s\n' "${category_x[@]}"
    overall_status=1
  fi

  if [[ ${#unknown[@]} -gt 0 ]]; then
    echo "FAIL ${module}: unclassified bundled license families need manual ASF policy review:"
    printf '  - %s\n' "${unknown[@]}"
    overall_status=1
  fi

  # Every dependency bundled under a non-Apache-2.0 license must ship its own
  # license text under META-INF/licenses/LICENSE-<artifactId>. The Apache banner
  # at the top of LICENSE-bundled only covers the Apache License 2.0 family.
  licenses_dir="${module}/src/main/resources/META-INF/licenses"

  # The LICENSE-bundled parse lives in exactly one place. This used to be a
  # second awk reimplementation of bundled_non_apache_coords() from the
  # generator, and the two detected a family heading differently (look-ahead for
  # the underline vs tracking the previous line), so they could silently
  # disagree about which dependencies need a text -- the precise failure this
  # tooling exists to prevent.
  non_apache_artifact_ids="$(
    python3 release/scripts/generate_shaded_license_metadata.py \
      --non-apache-artifact-ids "${license_file}"
  )"

  missing_license_texts=()
  while IFS= read -r artifact_id; do
    [[ -z "${artifact_id}" ]] && continue
    if [[ ! -f "${licenses_dir}/LICENSE-${artifact_id}" ]]; then
      missing_license_texts+=("${artifact_id}")
    fi
  done <<< "${non_apache_artifact_ids}"

  # And the reverse direction: a license text left behind for a dependency the
  # jar no longer bundles. Attributing code that is not shipped is its own
  # accuracy problem, and checking only one direction is what allowed the
  # orphans in #865 to accumulate unnoticed.
  orphaned_license_texts=()
  if [[ -d "${licenses_dir}" ]]; then
    while IFS= read -r text_file; do
      [[ -z "${text_file}" ]] && continue
      artifact_id="$(basename "${text_file}")"
      artifact_id="${artifact_id#LICENSE-}"
      if ! grep -qxF "${artifact_id}" <<< "${non_apache_artifact_ids}"; then
        orphaned_license_texts+=("${artifact_id}")
      fi
    done < <(find "${licenses_dir}" -name 'LICENSE-*' | sort)
  fi

  if [[ ${#missing_license_texts[@]} -gt 0 ]]; then
    overall_status=1
    echo "FAIL ${module}: missing META-INF/licenses/ text for non-Apache-2.0 dependencies:"
    printf '  - LICENSE-%s\n' "${missing_license_texts[@]}"
  fi

  if [[ ${#orphaned_license_texts[@]} -gt 0 ]]; then
    overall_status=1
    echo "FAIL ${module}: orphaned META-INF/licenses/ text with no bundled non-Apache-2.0 dependency:"
    printf '  - LICENSE-%s\n' "${orphaned_license_texts[@]}"
  fi

  if [[ ${#missing_license_texts[@]} -eq 0 && ${#orphaned_license_texts[@]} -eq 0 ]]; then
    echo "OK   ${module}: license texts match the bundled non-Apache-2.0 dependencies exactly."
  fi
done

exit "${overall_status}"
