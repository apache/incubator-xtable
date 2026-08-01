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
### Checking for DISCLAIMER-WIP
echo "Checking for DISCLAIMER-WIP"
disclaimerFile="./DISCLAIMER-WIP"
if [ ! -f "$disclaimerFile" ]; then
  echo "DISCLAIMER-WIP file not be present [ERROR]"
  exit 1
fi
echo -e "\t\tDISCLAIMER-WIP file exists ? [OK]\n"

### Checking for LICENSE and NOTICE
echo "Checking for LICENSE and NOTICE"
licenseFile="./LICENSE"
noticeFile="./NOTICE"
if [ ! -f "$licenseFile" ]; then
  echo "License file missing [ERROR]"
  exit 1
fi
echo -e "\t\tLicense file exists ? [OK]"

if [ ! -f "$noticeFile" ]; then
  echo "Notice file missing [ERROR]"
  exit 1
fi
echo -e "\t\tNotice file exists ? [OK]\n"

### Checking NOTICE distribution years
# The copyright range has to cover the year the release is published, otherwise
# every release cut after a new year ships a stale NOTICE (see XTABLE-692 and the
# 0.4.0-incubating-rc1 vote). Checking that the file merely exists is not enough.
echo "Checking NOTICE distribution years"
currentYear=$(date +%Y)
inceptionYear=$(sed -n 's:.*<inceptionYear>\(.*\)</inceptionYear>.*:\1:p' ./pom.xml | head -n 1)
copyrightLine=$(grep -E '^Copyright [0-9]{4}(-[0-9]{4})? The Apache Software Foundation$' "$noticeFile" | head -n 1)

if [ -z "$copyrightLine" ]; then
  echo "NOTICE does not contain a well-formed copyright line [ERROR]"
  echo -e "\t\texpected: Copyright ${inceptionYear:-<inceptionYear>}-${currentYear} The Apache Software Foundation"
  exit 1
fi

# Keep this portable: BSD/macOS sed has no GNU-style branch chaining, so split the
# "YYYY" / "YYYY-YYYY" range with parameter expansion instead.
noticeYears=$(echo "$copyrightLine" | sed -E 's/^Copyright ([0-9]{4}(-[0-9]{4})?) .*/\1/')
noticeStartYear="${noticeYears%%-*}"
noticeEndYear="${noticeYears##*-}"

if [ -n "$inceptionYear" ] && [ "$noticeStartYear" != "$inceptionYear" ]; then
  echo "NOTICE copyright starts at ${noticeStartYear} but pom.xml <inceptionYear> is ${inceptionYear} [ERROR]"
  echo -e "\t\tfound: ${copyrightLine}"
  exit 1
fi

if [ "$noticeEndYear" != "$currentYear" ]; then
  echo "NOTICE copyright must cover the release year ${currentYear} but ends at ${noticeEndYear} [ERROR]"
  echo -e "\t\tfound:    ${copyrightLine}"
  echo -e "\t\texpected: Copyright ${noticeStartYear}-${currentYear} The Apache Software Foundation"
  exit 1
fi
echo -e "\t\tNOTICE copyright covers ${currentYear} ? [OK]\n"

### Licensing Check
echo "Performing custom Licensing Check "
numfilesWithNoLicense=`find . -iname '*' -type f | grep -v NOTICE | grep -v LICENSE | grep -v '.jpg' | grep -v '.json' | grep -v '.hfile' | grep -v '.data' | grep -v '.commit' | grep -v emptyFile | grep -v DISCLAIMER | grep -v KEYS | grep -v '.mailmap' | grep -v '.sqltemplate' | grep -v 'banner.txt' | grep -v "fixtures" | xargs grep -L "Licensed to the Apache Software Foundation (ASF)" | wc -l`
if [ "$numfilesWithNoLicense" -gt  "0" ]; then
  echo "There were some source files that did not have Apache License [ERROR]"
  find . -iname '*' -type f | grep -v NOTICE | grep -v LICENSE | grep -v '.jpg' | grep -v '.json' | grep -v '.hfile' | grep -v '.data' | grep -v '.commit' | grep -v emptyFile | grep -v DISCLAIMER | grep -v '.sqltemplate' | grep -v KEYS | grep -v '.mailmap' | grep -v 'banner.txt' | grep -v "fixtures" | xargs grep -L "Licensed to the Apache Software Foundation (ASF)"
  exit 1
fi
echo -e "\t\tLicensing Check Passed [OK]\n"
