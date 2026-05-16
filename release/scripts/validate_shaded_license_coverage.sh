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

SHADE_MODULES=()
while IFS= read -r module; do
  module_dir="$(dirname "${module}")"
  if [[ "${module_dir}" != "." ]]; then
    SHADE_MODULES+=("${module_dir}")
  fi
done < <(
  rg -l '<artifactId>maven-shade-plugin</artifactId>' --glob 'pom.xml' \
    | sort
)

if [[ ${#SHADE_MODULES[@]} -eq 0 ]]; then
  echo "No modules with maven-shade-plugin were found."
  exit 0
fi

missing_runtime_trees=()
for module in "${SHADE_MODULES[@]}"; do
  if [[ ! -f "${module}/target/dependency-tree-runtime.txt" ]]; then
    missing_runtime_trees+=("${module}")
  fi
done

if [[ ${#missing_runtime_trees[@]} -gt 0 ]]; then
  module_csv="$(IFS=,; printf '%s' "${missing_runtime_trees[*]}")"
  ./mvnw -pl "${module_csv}" -DskipTests dependency:tree -Dscope=runtime -DoutputType=text -DoutputFile=target/dependency-tree-runtime.txt
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
    echo "FAIL ${module}: shade <includes> must exactly match runtime dependencies from ${tree_file}."
    sed 's/^/  /' "${diff_output_file}"
    overall_status=1
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
done

exit "${overall_status}"
