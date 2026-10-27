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
    "Apache License 2.0"|"Apache Software License 1.1"|"BSD 2-Clause"|"BSD 3-Clause"|"BSD 2-Clause License"|"BSD 3-Clause License"|"The BSD 3-Clause License"|"BSD licence"|"MIT License"|"MIT-0"|"Bouncy Castle License"|"Javolution License"|"ISC"|"ICU"|"University of Illinois/NCSA"|"W3C Software License"|"X.Net"|"zlib/libpng"|"Eclipse Distribution License - v 1.0"|"Public Domain")
      printf 'A\n'
      ;;
    "CDDL"|"Common Development and Distribution License"|"Common Development and Distribution License 1.0"|"Common Development and Distribution License 1.1"|"CDDL + GPLv2 with classpath exception"|"GPL 2.0 with Classpath Exception"|"Mozilla Public License 1.0"|"Mozilla Public License 1.1"|"Mozilla Public License 2.0"|"EPL 1.0"|"EPL 2.0"|"Eclipse Public License 1.0"|"Eclipse Public License 2.0"|"GPL-2.0 with GNU ClasspathException"|"Common Public License Version 1.0")
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

SHADE_MODULES=()
while IFS= read -r module; do
  module_dir="$(dirname "${module}")"
  if [[ "${module_dir}" != "." ]]; then
    if ! should_skip_module "${module_dir}"; then
      SHADE_MODULES+=("${module_dir}")
    fi
  fi
done < <(git grep -l '<artifactId>maven-shade-plugin</artifactId>' -- 'pom.xml' '*/pom.xml')

# Finding nothing means discovery broke, so fail rather than pass vacuously.
if [[ ${#SHADE_MODULES[@]} -eq 0 ]]; then
  echo "FAIL no modules with maven-shade-plugin were found."
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
  license_file="${module}/target/bundled-archive-resources/META-INF/LICENSE"
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
    echo "FAIL ${module}: shade <includes> must exactly match runtime dependencies from ${tree_file}."
    sed 's/^/  /' "${diff_output_file}"
    overall_status=1
  fi

  rm -f "${include_set_file}" "${runtime_set_file}" "${diff_output_file}"

  # The LICENSE render lists the artifacts named in xtable.bundled.artifactIds, so that list must
  # name exactly the shaded artifacts.
  listed_file="$(mktemp)"
  shaded_file="$(mktemp)"
  sed -n 's:.*<xtable.bundled.artifactIds>\(.*\)</xtable.bundled.artifactIds>.*:\1:p' "${pom_file}" \
    | tr ',' '\n' | sed '/^$/d' | while IFS= read -r artifact_id; do
      resolve_property "${artifact_id}"
    done | sort -u > "${listed_file}"
  printf '%s\n' "${includes[@]}" | while IFS= read -r include; do
    resolve_property "${include}" | cut -d: -f2
  done | sort -u > "${shaded_file}"
  if ! diff -u "${shaded_file}" "${listed_file}" > "${listed_file}.diff"; then
    echo "FAIL ${module}: xtable.bundled.artifactIds must name exactly the artifacts in the shade <includes>."
    sed 's/^/  /' "${listed_file}.diff"
    overall_status=1
  else
    echo "OK   ${module}: xtable.bundled.artifactIds matches the shade <includes>."
  fi
  rm -f "${listed_file}" "${shaded_file}" "${listed_file}.diff"

  if [[ ! -f "${license_file}" ]]; then
    echo "FAIL ${module}: ${license_file} is missing; build the module to render it."
    overall_status=1
    continue
  fi

  # A bundle whose LICENSE transformer names a missing file ships without META-INF/LICENSE, and the
  # shade plugin does not say so.
  for jar in "${module}"/target/*.jar; do
    case "${jar}" in
      *-sources.jar|*-tests.jar|*-javadoc.jar|*/original-*.jar) continue ;;
    esac
    [[ -f "${jar}" ]] || continue
    for entry in META-INF/LICENSE META-INF/NOTICE; do
      count="$(unzip -Z1 "${jar}" | grep -cx "${entry}" || true)"
      if [[ "${count}" -ne 1 ]]; then
        echo "FAIL ${module}: $(basename "${jar}") has ${count} ${entry} entries, expected 1."
        overall_status=1
      fi
    done
  done

  unmapped=()
  while IFS= read -r entry; do
    unmapped+=("${entry}")
  done < <(sed -n 's/^UNMAPPED-LICENSE: //p' "${license_file}")
  if [[ ${#unmapped[@]} -gt 0 ]]; then
    echo "FAIL ${module}: bundled artifacts with no license in their POM; add them to supplemental-models.xml:"
    printf '  - %s\n' "${unmapped[@]}"
    overall_status=1
  fi

  families=()
  while IFS= read -r family; do
    families+=("${family}")
  done < <(
    sed -n '/^BUNDLED DEPENDENCIES UNDER OTHER LICENSES$/,$p' "${license_file}" \
      | sed -nE 's/^  [^ ]+:[^ ]+ +(.*)\. See META-INF\/licenses\/.*$/\1/p' \
      | sort -u
  )

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
done

exit "${overall_status}"
