#!/usr/bin/env bash
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
set -euo pipefail

# Color and timestamp logging helpers
log() {
  local color="$1"; shift
  local msg="$1"; shift
  local now
  now="$(date '+%Y-%m-%d %H:%M:%S')"
  printf "\033[%sm[%s] %s\033[0m\n" "$color" "$now" "$msg" "$@"
}
log_info() { log "1;34" "$1"; }
log_success() { log "1;32" "$1"; }
log_warn() { log "1;33" "$1"; }
log_error() { log "1;31" "$1"; }

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

  if [[ "${jar_count}" -eq 0 ]]; then
    log_info "No bundled jar found in ${module_dir}/target. Skipping."
    return 1
  fi
  if [[ "${jar_count}" -ne 1 ]]; then
    log_error "Expected exactly one bundled jar in ${module_dir}/target, found ${jar_count}."
    return 2
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
    log_error "[${module_dir}] Missing required entry ${jar_entry} in ${jar_path}"
    exit 1
  fi

  local extracted_path
  extracted_path="$(make_temp_file)"
  unzip -p "${jar_path}" "${jar_entry}" > "${extracted_path}"

  if ! cmp -s "${source_path}" "${extracted_path}"; then
    log_error "[${module_dir}] ${jar_entry} in ${jar_path} does not match ${source_path}"
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
    log_error "[${module_dir}] Bundled dependencies are missing from ${license_path}:"
    sed 's/^/  - /' "${missing_coords_path}"
    log_error "Add the coordinates above to ${license_path} under the appropriate license section."
    log_error "Review $(dirname "${license_path}")/NOTICE-bundled too if any of those dependencies carry NOTICE text."
    exit 1
  fi
}

print_summary() {
  local module_dir="$1"
  log_success "[${module_dir}] Bundled license verification PASSED."
}

main() {
  if [[ $# -ne 1 ]]; then
    log_error "Usage: $0 <module-dir>"
    exit 2
  fi
  local module_dir="$1"
  local meta_inf_dir="${module_dir}/src/main/resources/META-INF"
  local jar_path

  if ! jar_path="$(ensure_single_bundled_jar "${module_dir}")"; then
    log_info "[${module_dir}] No bundled jar to check. Skipping."
    exit 0
  fi

  if [[ ! -d "${meta_inf_dir}" ]]; then
    log_error "[${module_dir}] META-INF directory missing but bundled jar exists. Failing."
    exit 1
  fi

  log_info "[${module_dir}] Checking META-INF resources in bundled jar."
  verify_module_meta_inf_resources "${jar_path}" "${module_dir}" "${meta_inf_dir}"

  log_info "[${module_dir}] Checking LICENSE-bundled covers all bundled dependencies."
  verify_bundled_coordinates_are_in_license "${jar_path}" "${module_dir}" "${meta_inf_dir}/LICENSE-bundled"

  print_summary "${module_dir}"
}

main "$@"
