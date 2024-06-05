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

# Guide on CI infrastructure

## Context
Apache XTable (incubating) uses Azure DevOps and Azure infrastructure for its CI, providing more flexibility by not being constrained to the Apache org's shared quota for CI. 
This approach is similar to those adopted by Apache Flink and Apache Hudi communities.

## Overview
We have created [apachextable-ci](https://github.com/apachextable-ci) to manage all CI-related repositories:

- [xtable-mirror](https://github.com/apachextable-ci/xtable-mirror): This repository mirrors the official repository (apache/incubator-xtable) to build commits merged into master and release-* branches.
- [xtable-branch-ci](https://github.com/apachextable-ci/xtable-branch-ci): This repository is dedicated to building branches created PRs. Its main branch remains at the commit of the branch's creation and does not require synchronization with the official repository.

The diagram below gives an overview of the infrastructure.

!TODO


## Azure VM
We are running a B1s VM instance in this [Azure XTable account]().

### Bootstrap Steps

```sh
sudo apt update -y
sudo apt install -y git cron openjdk-8-jdk maven
git clone git@github.com:apachextable-ci/git-repo-sync.git
git clone git@github.com:apachextable-ci/ci-bot.git
cd ci-bot
mvn clean install
```

### Mirroring Master & Release Commits
Use `crontab -e` to schedule the mirroring job, currently configured to mirror `main` and release commits to the `apachextable-ci` repository.

```sh
*/10 * * * * $HOME/git-repo-sync/sync_repo.sh > /dev/null 2>&1
```

### Scanning PRs and Triggering Branch Builds
Run `$HOME/git-repo-sync/run_cibot.sh` to start the CI branch builds in the background.

### Maintenance
Manage the background process with `htop`.
Typical maintenance steps include killing the process from `htop`, cleaning up `~/ci-bot.log`, and re-running the script.

Check the expiry date of Azure and GitHub tokens and update them accordingly. They are used in `$HOME/git-repo-sync/run_cibot.sh`.

### Potential Issues
- Mirrored repositories (e.g., master, release-*) not being pushed, leading to failure in mirrored CI execution.
	- Issues with `git fetch` or `push` in `$HOME/git-repo-sync/sync_repo.sh`. Manually execute the git commands and troubleshoot as needed.

## Azure Pipelines
There are two pipelines defined in this [Azure DevOps project](https://dev.azure.com/apache-xtable-ci-org/apache-xtable-ci).

- xtable-mirror: for master/release version builds
- xtable-branch-ci: for PR builds

For each `xtable-branch-ci` build, xtable-bot will post and update comments on its corresponding PR as follows.

!TODO

PR reviewers should consider the results of this CI report as one of the merging criteria.

### Get Help
- Azure DevOps docs:
	- [Pipelines](https://docs.microsoft.com/en-us/azure/devops/pipelines/?view=azure-devops).
	- [Troubleshooting](https://docs.microsoft.com/en-us/azure/devops/pipelines/troubleshooting/troubleshooting?view=azure-devops).
- For community support, raise questions on [developercommunity.visualstudio.com](https://developercommunity.visualstudio.com/).
