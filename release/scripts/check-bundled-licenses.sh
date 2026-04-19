#!/usr/bin/env bash

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

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

MODULE_CONFIGS=(
  "xtable-aws|xtable-aws/src/main/resources/META-INF"
  "xtable-hive-metastore|xtable-hive-metastore/src/main/resources/META-INF"
  "xtable-hudi-support/xtable-hudi-support-extensions|xtable-hudi-support/xtable-hudi-support-extensions/src/main/resources/META-INF"
)

BUILD_TARGETS="xtable-aws,xtable-hive-metastore,xtable-hudi-support/xtable-hudi-support-extensions"

cleanup_files=()

cleanup() {
  if [[ ${#cleanup_files[@]} -gt 0 ]]; then
    rm -f "${cleanup_files[@]}"
  fi
}

trap cleanup EXIT

make_temp_file() {
  local tmp_file
  tmp_file="$(mktemp)"
  cleanup_files+=("${tmp_file}")
  printf '%s\n' "${tmp_file}"
}

ensure_single_bundled_jar() {
  local module_dir="$1"
  local jar_count
  local jar_path

  jar_count="$(find "${module_dir}/target" -maxdepth 1 -type f -name '*-bundled.jar' | wc -l | tr -d ' ')"

  if [[ "${jar_count}" -ne 1 ]]; then
    printf 'Expected exactly one bundled jar in %s/target, found %s\n' "${module_dir}" "${jar_count}" >&2
    exit 1
  fi

  jar_path="$(find "${module_dir}/target" -maxdepth 1 -type f -name '*-bundled.jar' | sort | head -n 1)"
  printf '%s\n' "${jar_path}"
}

jar_has_entry() {
  local jar_path="$1"
  local jar_entry="$2"

  unzip -Z1 "${jar_path}" "${jar_entry}" >/dev/null 2>&1
}

verify_jar_entry_matches_source() {
  local jar_path="$1"
  local jar_entry="$2"
  local source_path="$3"
  local module_dir="$4"

  if ! jar_has_entry "${jar_path}" "${jar_entry}"; then
    printf '[%s] Missing required entry %s in %s\n' "${module_dir}" "${jar_entry}" "${jar_path}" >&2
    exit 1
  fi

  local extracted_path
  extracted_path="$(make_temp_file)"
  unzip -p "${jar_path}" "${jar_entry}" > "${extracted_path}"

  if ! cmp -s "${source_path}" "${extracted_path}"; then
    printf '[%s] %s in %s does not match %s\n' "${module_dir}" "${jar_entry}" "${jar_path}" "${source_path}" >&2
    diff -u "${source_path}" "${extracted_path}" || true
    exit 1
  fi
}

verify_module_meta_inf_resources() {
  local jar_path="$1"
  local module_dir="$2"
  local meta_inf_dir="$3"

  while IFS= read -r source_path; do
    local relative_path
    local jar_entry

    relative_path="${source_path#"${meta_inf_dir}/"}"
    jar_entry="META-INF/${relative_path}"
    verify_jar_entry_matches_source "${jar_path}" "${jar_entry}" "${source_path}" "${module_dir}"
  done < <(find "${meta_inf_dir}" -type f | sort)
}

extract_bundled_coordinates() {
  local jar_path="$1"

  while IFS= read -r pom_properties; do
    local coordinate

    coordinate="$(
      unzip -p "${jar_path}" "${pom_properties}" | tr -d "\r" | awk -F= '
        $1 == "groupId" { group_id = $2 }
        $1 == "artifactId" { artifact_id = $2 }
        $1 == "version" { version = $2 }
        END {
          if (group_id != "" && artifact_id != "" && version != "" && group_id !~ /^org\.apache\.xtable$/) {
            print group_id ":" artifact_id ":" version
          }
        }
      '
    )"

    if [[ -n "${coordinate}" ]]; then
      printf '%s\n' "${coordinate}"
    fi
  done < <(zipinfo -1 "${jar_path}" | awk '/^META-INF\/maven\/.*\/pom.properties$/') | sort -u
}

extract_license_coordinates() {
  local license_path="$1"

  tr -d '\r' < "${license_path}" \
    | grep -Eo '[[:alnum:]_.-]+:[[:alnum:]_.-]+:(jar:)?[[:alnum:]_.+-]+' \
    | awk -F: '
      NF == 3 { print $1 ":" $2 ":" $3 }
      NF == 4 && $3 == "jar" { print $1 ":" $2 ":" $4 }
    ' \
    | sort -u
}

verify_bundled_coordinates_are_in_license() {
  local jar_path="$1"
  local module_dir="$2"
  local license_path="$3"
  local actual_coords_path
  local licensed_coords_path
  local missing_coords_path

  actual_coords_path="$(make_temp_file)"
  licensed_coords_path="$(make_temp_file)"
  missing_coords_path="$(make_temp_file)"

  extract_bundled_coordinates "${jar_path}" > "${actual_coords_path}"
  extract_license_coordinates "${license_path}" > "${licensed_coords_path}"

  comm -23 "${actual_coords_path}" "${licensed_coords_path}" > "${missing_coords_path}"

  if [[ -s "${missing_coords_path}" ]]; then
    printf '[%s] Bundled dependencies are missing from %s:\n' "${module_dir}" "${license_path}" >&2
    sed 's/^/  - /' "${missing_coords_path}" >&2
    printf '\nAdd the coordinates above to %s under the appropriate license section.\n' "${license_path}" >&2
    printf 'Review %s/NOTICE-bundled too if any of those dependencies carry NOTICE text.\n' "$(dirname "${license_path}")" >&2
    exit 1
  fi
}

main() {
  if [[ "${SKIP_BUNDLED_LICENSE_BUILD:-0}" != "1" ]]; then
    ./mvnw \
      -pl "${BUILD_TARGETS}" \
      -am \
      package \
      -DskipTests \
      -Dmaven.build.cache.enabled=false \
      -ntp \
      -B
  fi

  for config in "${MODULE_CONFIGS[@]}"; do
    IFS='|' read -r module_dir meta_inf_dir <<< "${config}"

    local_jar_path="$(ensure_single_bundled_jar "${module_dir}")"
    verify_module_meta_inf_resources "${local_jar_path}" "${module_dir}" "${meta_inf_dir}"
    verify_bundled_coordinates_are_in_license "${local_jar_path}" "${module_dir}" "${meta_inf_dir}/LICENSE-bundled"
  done

  printf 'Bundled license verification passed.\n'
}

main "$@"
