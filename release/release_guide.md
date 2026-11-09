<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Introduction

Apache XTable, currently an incubating project at the Apache Software Foundation (ASF), is a cross-table converter for table formats that facilitates omni-directional interoperability across data processing systems and query engines. 
This guide outlines the steps for releasing Apache XTable versions, following the ASF policies and incorporating practices from other successful ASF projects.

# Legal Reminders

Releasing software under the ASF banner requires adherence to the [ASF Release Policy](https://infra.apache.org/release-publishing.html) and [Incubator Release Guidelines](https://incubator.apache.org/guides/releasemanagement.html#best-practice). 
Please ensure all releases comply with these guidelines, focusing on proper licensing, source code distribution, and community approval.

# Overview

The release process consists of several steps:

1. Decide to release
2. Prepare for the release
3. Build a release candidate
4. Vote on the release candidate
5. During vote process, run validation tests
6. If necessary, fix any issues and go back to step 3.
7. Finalize the release
8. Promote the release

# Decide to release

Anybody can propose a release on the dev@ mailing list, giving a solid argument and nominating a committer as the
Release Manager (including themselves). There’s no formal process, no vote requirements, and no timing requirements. Any
objections should be resolved by consensus before starting the release.

# Prepare for the release

_Before your first release, you should perform one-time configuration steps. This will set up your security keys for
signing the release and access to various release repositories._

To prepare for each release, you should audit the project status in the Github issue tracker for any release-blockers and ensure all of them are resolved.
Finally, you should create a release branch from which individual release candidates will be built.

**NOTE**: If you are
using [GitHub two-factor authentication](https://help.github.com/articles/securing-your-account-with-two-factor-authentication-2fa/)
and haven’t configure HTTPS access, please
follow [the guide](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/) to configure
command line access.

## One-time Setup Instructions

You need to have a GPG key to sign the release artifacts. Please be aware of the
ASF-wide [release signing guidelines](https://www.apache.org/dev/release-signing.html). If you don’t have a GPG key
associated with your Apache account, please follow the instructions below.

### Create GPG public key with apache email

- Install gpg using [https://gpgtools.org/](https://gpgtools.org/).
- Create gpg key with your apache emailId. 
- Export the KeyId into an environment variable KEY_ID

```shell
LOCAL_SVN_DIR=local_svn_dir
mkdir -p ~/$LOCAL_SVN_DIR && cd $LOCAL_SVN_DIR
svn checkout https://dist.apache.org/repos/dist/dev/incubator/xtable && cd xtable
email=yourapacheemailusername@apache.org
(gpg --list-sigs ${email} && gpg --armor --export ${name}) >> KEYS
svn --username=${apache_username} --password=${passowrd} commit -m "Add xtable directory and keys"
```

### Submit your GPG public key into MIT PGP Public Key Server

In order to make yourself have the right permission to stage java artifacts in Apache Nexus staging repository, please
submit your GPG public key into [MIT PGP Public Key Server](http://pgp.mit.edu:11371/).

Also send public key to ubuntu server via

```shell
gpg --keyserver hkp://keyserver.ubuntu.com --send-keys ${KEY_ID} # send public key to ubuntu server
gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys ${KEY_ID} # verify
```

would also refer
to [stackoverflow](https://stackoverflow.com/questions/19462617/no-public-key-key-with-id-xxxxx-was-not-able-to-be-located-oss-sonatype-org)

### Access to Apache Nexus repository

Configure access to the Apache Nexus repository, which enables final deployment of releases to the Maven Central
Repository.

1. You log in with your Apache account.
2. Confirm you have appropriate access by finding org.apache.xtable under Staging Profiles.
3. Navigate to your Profile (top right dropdown menu of the page).
4. Choose User Token from the dropdown, then click Access User Token. Copy a snippet of the Maven XML configuration
   block.
5. Insert this snippet twice into your global Maven settings.xml file, typically ${HOME}/.m2/settings.xml. The end
   result should look like this, where TOKEN_NAME and TOKEN_PASSWORD are your secret tokens:

```xml
<settings>
   <servers>
     <server>
       <id>apache.releases.https</id>
       <username>TOKEN_NAME</username>
       <password>TOKEN_PASSWORD</password>
     </server>
     <server>
       <id>apache.snapshots.https</id>
       <username>TOKEN_NAME</username>
       <password>TOKEN_PASSWORD</password>
     </server>
   </servers>
 </settings>
```


## Create a release branch in apache/incubator-xtable repository

**Skip this step if it is a bug fix release. But do set the env variables.**

Release candidates are built from a release branch. As a final step in preparation for the release, you should create the release branch, push it to the Apache code repository, and update version information on the original branch.

Export Some Environment variables in the terminal where you are running the release scripts

- export RELEASE_VERSION=<RELEASE_VERSION_TO_BE_PUBLISHED>-incubating
- export NEXT_VERSION=<NEW_VERSION_IN_MASTER>
- export RC_NUM=<release_candidate_num_starting_from_1>
- export RELEASE_BRANCH=release-<RELEASE_VERSION_TO_BE_PUBLISHED>-rc<release_candidate_num_starting_from_1>

Use cut_release_branch.sh to cut a release branch

- Script: [cut_release_branch.sh](https://github.com/apache/incubator-xtable/blob/master/scripts/release/cut_release_branch.sh)

Usage

```shell
# Cut a release branch
./release/scripts/cut_release_branch.sh \
--release=${RELEASE_VERSION} \
--next_release=${NEXT_VERSION} \
--rc_num=${RC_NUM}
# Show help page
./incubator-xtable/release/scripts/cut_release_branch.sh -h
```

## Verify that a release build works

On the release branch, run the release profile without signing to check that the build, the javadoc and the source release work:

```shell
./mvnw clean install -Papache-release -DskipTests -Dgpg.skip -Dmaven.build.cache.enabled=false
```

The build writes `target/xtable-${RELEASE_VERSION}-source-release.tar.gz` and `.zip`, each with a `.sha512`.

# Build a release candidate

The core of the release process is the build-vote-fix cycle. Each cycle produces one release candidate. The Release Manager repeats this cycle until the community approves one release candidate, which is then finalized.

The source release, the official subject of the vote, is a Maven build output. The `apache-release` profile, inherited from the ASF parent POM `org.apache:apache` and extended in the root `pom.xml`, does the following in one build:

- Packs the source tree into `xtable-${RELEASE_VERSION}-source-release.tar.gz` and `.zip`, using [release/assembly/source-release.xml](assembly/source-release.xml). Entries carry the timestamp of `project.build.outputTimestamp` and no owner or extended attributes, so the archive doesn't depend on the machine that builds it.
- Writes a `.sha512` for each archive, in the `<hash>  <file>` format that `sha512sum -c` and `shasum -a 512 -c` read.
- Builds the jars, sources jars and javadoc jars.
- Signs every artifact with `maven-gpg-plugin`.
- Deploys all of it, the source release included, to the Nexus staging repository.

The archive contains the working directory of the build, minus the excludes in the descriptor, so always build from a fresh clone of the release tag.

Set up the environment variables for this candidate. Start with `RC_NUM` equal to 1 and increment it for each candidate.

```shell
export RELEASE_VERSION=<RELEASE_VERSION_TO_BE_PUBLISHED>-incubating   # for example 0.5.0-incubating
export RC_NUM=<RC_NUM>
export RELEASE_BRANCH=release-${RELEASE_VERSION}-rc${RC_NUM}
export RELEASE_TAG=release-${RELEASE_VERSION}-rc${RC_NUM}
export KEY_ID=<FINGERPRINT_OF_YOUR_APACHE_KEY>
```

1. Create and push the tag.

   ```shell
   git checkout ${RELEASE_BRANCH}
   git tag -s ${RELEASE_TAG} -u ${KEY_ID} -m "${RELEASE_VERSION} rc${RC_NUM}"
   git push origin refs/tags/${RELEASE_TAG}
   ```

   The release branch and the tag share a name, so push the tag by its full ref. To redo a candidate whose staging repository you have dropped, delete the tag first with `git tag -d ${RELEASE_TAG}` and `git push --delete origin refs/tags/${RELEASE_TAG}`.

2. Build and deploy from a fresh clone of the tag. Use JDK 11.

   ```shell
   git clone https://github.com/apache/incubator-xtable.git xtable-${RELEASE_TAG}
   cd xtable-${RELEASE_TAG}
   git checkout --detach refs/tags/${RELEASE_TAG}
   export GPG_TTY=$(tty)
   ./release/scripts/deploy_staging_jars.sh -Dgpg.keyname=${KEY_ID}
   ```

   The script refuses to run unless `HEAD` is a tag and the checkout has no modified, untracked or ignored files. It then runs:

   ```shell
   ./mvnw clean deploy -Papache-release -DskipTests -DretryFailedDeploymentCount=10 -Dmaven.build.cache.enabled=false -Dgpg.keyname=${KEY_ID}
   ```

   `-Dmaven.build.cache.enabled=false` is required: the build cache re-attaches the sources jar and the deploy fails with "duplicated artifacts attached". `gpg-agent` asks for the passphrase; to pass it non-interactively, export `MAVEN_GPG_PASSPHRASE`. Keep your IP address stable during the upload, otherwise Nexus opens a second staging repository.

3. Validate the source release that the build left in `target/`. The script checks the checksums and signatures, rejects macOS metadata (`._*`, `__MACOSX`, `.DS_Store`), extended-attribute headers and files outside the top-level directory, then unpacks the tarball and runs the binary file, license and RAT checks.

   ```shell
   cd release/scripts
   ./validate_staged_release.sh --release=${RELEASE_VERSION} --rc_num=${RC_NUM} --local_dir=../../target --verbose
   cd ../..
   ```

   Also build and test the unpacked source with `./mvnw clean package` before you close the staging repository.

4. Review and close the staging repository.
    1. Log in to [Apache Nexus](https://repository.apache.org/), open **Staging Repositories** and find the open `orgapachextable-NNNN` entry. It contains every module's pom, jar, sources and javadoc jar, and under `org/apache/xtable/xtable/${RELEASE_VERSION}/` the files `xtable-${RELEASE_VERSION}-source-release.tar.gz` and `.zip` with their `.asc` and `.sha512`.
    2. Until you close it, a failed deploy can be repeated into the same repository. Once everything is in place, close it with the description:
       > Apache XTable (incubating), version `${RELEASE_VERSION}`, release candidate `${RC_NUM}`.
    3. Check the staged bundles:
       ```shell
       ./release/scripts/validate_staged_bundles.sh orgapachextable-NNNN ${RELEASE_VERSION} 2>&1 | tee -a /tmp/validate_staged_bundles_output.txt
       ```

5. Stage the source release on [dist.apache.org](https://dist.apache.org/repos/dist/dev/incubator/xtable). Copy it from the closed staging repository, so the voted files are the ones in Nexus.

   ```shell
   STAGING_URL=https://repository.apache.org/content/repositories/orgapachextable-NNNN/org/apache/xtable/xtable/${RELEASE_VERSION}
   svn checkout https://dist.apache.org/repos/dist/dev/incubator/xtable --depth=immediates
   mkdir xtable/${RELEASE_VERSION}-rc${RC_NUM}
   cd xtable/${RELEASE_VERSION}-rc${RC_NUM}
   for ext in tar.gz tar.gz.asc tar.gz.sha512 zip zip.asc zip.sha512; do
     curl -fsSLO ${STAGING_URL}/xtable-${RELEASE_VERSION}-source-release.${ext}
   done
   shasum -a 512 -c *.sha512
   for f in *.tar.gz *.zip; do gpg --verify ${f}.asc ${f}; done
   # the same bytes as the local build
   for f in *.tar.gz *.zip; do cmp ${f} <PATH_TO_CLONE>/target/${f}; done
   cd ..
   svn add ${RELEASE_VERSION}-rc${RC_NUM}
   svn commit -m "Add Apache XTable ${RELEASE_VERSION} rc${RC_NUM}"
   ```

   Replace `NNNN` with the number of the staging repository and `<PATH_TO_CLONE>` with the clone from step 2.

6. Validate the files on dist.apache.org:

   ```shell
   cd release/scripts
   ./validate_staged_release.sh --release=${RELEASE_VERSION} --rc_num=${RC_NUM} --verbose
   ```

## Checklist to proceed to the next step

1. Maven artifacts deployed to the staging repository of repository.apache.org
2. Source distribution deployed to the dev repository of dist.apache.org and validated

# Vote on the release candidate

Once you have built and individually reviewed the release candidate, please share it for the community-wide(dev@incubator-xtable ) review. Please review foundation-wide voting guidelines for more information.

Start the review-and-vote thread on the dev@ mailing list. Here’s an email template; please adjust as you see fit.

```
From: Release Manager

To: dev@incubator-xtable.apache.org 

Subject: [VOTE] Release 1.2.3, release candidate #3

 

Hi everyone,

Please review and vote on the release candidate #3 for the version 1.2.3, as follows:

[ ] +1, Approve the release

[ ] -1, Do not approve the release (please provide specific comments)

 

The complete staging area is available for your review, which includes:

* GH release notes [1],

* the official Apache source release and binary convenience releases to be deployed to dist.apache.org [2], which are signed with the key with fingerprint FFFFFFFF [3],

* all artifacts to be deployed to the Maven Central Repository [4],

* source code tag "1.2.3-rc3" [5],

 

The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

 

Thanks,

Release Manager

 

[1] link

[2] link

[3] https://dist.apache.org/repos/dist/release/incubator/xtable/KEYS

[4] link

[5] link

[6] link
```

If there are any issues found in the release candidate, reply on the vote thread to cancel the vote, and there’s no need to wait 72 hours if any issues found. Proceed to the Fix Issues step below and address the problem. However, some issues don’t require cancellation. For example, if an issue is found in the website, just correct it on the spot and the vote can continue as-is.

If there are no issues, reply on the vote thread to close the voting. Then, tally the votes in a separate email. Here’s an email template; please adjust as you see fit.

```
From: Release Manager

To: dev@xtable.apache.org

Subject: [RESULT] [VOTE] Release 1.2.3, release candidate #3

 

I'm happy to announce that we have unanimously approved this release.

 

There are XXX approving votes, XXX of which are binding:

* approver 1

* approver 2

* approver 3

* approver 4

 

There are no disapproving votes.

 

Thanks everyone!
```

## Checklist to proceed to the finalization step

Community votes to release the proposed candidate, with at least three approving PMC votes

# Fix any issues

Any issues identified during the community review and vote should be fixed in this step.

Code changes should be proposed as standard pull requests to the master branch and reviewed using the normal contributing process. Then, relevant changes should be cherry-picked into the release branch. The cherry-pick commits should then be proposed as the pull requests against the release branch, again reviewed and merged using the normal contributing process.

Once all issues have been resolved, you should go back and build a new release candidate with these changes.

## Checklist to proceed to the next step

Issues identified during vote have been resolved, with fixes committed to the release branch.

# Finalize the release

Once the release candidate has been reviewed and approved by the community, the release should be finalized. This involves the final deployment of the release candidate to the release repositories, merging of the website changes, etc.

## Current follow the below steps to finalize the release.

1. Drop all RC orgapachextable-XXX in [Apache Nexus Staging Repositories](https://repository.apache.org/#stagingRepositories) that haven't been approved.
2. Checkout to the release branch for the approved rc and move it to one without the rc suffix. Eg: `release-0.1.0-incubating-rc4 to release-0.1.0-incubating`
    1. git commit -am "[MINOR] Update release version to reflect published version  ${RELEASE_VERSION}"
    2. git push origin release-${RELEASE_VERSION}
3. Copy the source distributions for the approved release candidate from https://dist.apache.org/repos/dist/dev/incubator/xtable/ to https://dist.apache.org/repos/dist/release/incubator
   Only PMC will have access to this repo. So, if you are not a PMC, do get help from someone who is.
    1. svn checkout https://dist.apache.org/repos/dist/release/incubator/xtable --depth=immediates, if you would not checkout, please try svn checkout https://dist.apache.org/repos/dist/release/incubator/xtable again.
    2. Make a directory for the new release:
       ```shell
       mkdir xtable/${RELEASE_VERSION}
       ```
    3. Copy incubator-xtable source distributions, hashes, and GPG signature:
       ```shell
       mv <svn-dev-xtable-dir>/approved-rcX/* xtable/${RELEASE_VERSION}
       ```
    4. Add and commit all the files.
       ```shell
       cd xtable
       svn add ${RELEASE_VERSION}
       svn commit
       ```
    5. Verify that files are [present](https://dist.apache.org/repos/dist/release/xtable)
4. Use the Apache Nexus repository to release the staged binary artifacts to the Maven Central repository. In the Staging Repositories section, find the relevant release candidate orgapachextable-XXX entry and click `Release`.
   > Note: make sure and check you click `Release` repo, it cannot be withdrawn.
5. Drop all other release candidates that are not being released. It can take up to 24 hours for the new release to show up in [Maven Central repository](https://search.maven.org/search?q=g:org.apache.xtable).
6. After the artifacts are visible on Maven Central, update `website/releases/downloads.mdx`.
   1. Keep the signed source archive as the first and official Apache release download.
   2. Label Maven Central JARs as convenience binaries.
   3. Add direct links for the published, user-facing JARs and retain a version-filtered Maven search link for the complete artifact list.
   4. Verify every exact JAR URL before adding it. Do not assume that every repository module is published.
7. Finalize the Release in GH by providing the release date.
8. Update [DOAP]

# Promote the release

Once the release has been finalized, the last step of the process is to promote the release within the project and beyond. Please wait for 1h after finalizing the release in accordance with the [ASF release policy](http://www.apache.org/legal/release-policy.html#release-announcements).
We need to create a blog post in https://xtable.apache.org/ which provides details about the release including a high-level description of major changes 

## Apache mailing lists

Announce on the dev@ mailing list that the release has been finished.

Announce on the release on the user@ mailing list, listing major improvements and contributions.

Announce the release on the [announce@apache.org](announce@apache.org) mailing list. **NOTE**: put [announce@apache.org](announce@apache.org) in **bcc** to avoid disrupting people with followups.

Considering that announce@ ML has restrictions on what is published, we can follow this email template:

```
From: Release Manager

To: announce@apache.org

Subject: [ANNOUNCE] Apache incubator-xtable <VERSION> released

 

The Apache incubator-xtable team is pleased to announce the release of Apache

incubator-xtable <VERSION>.



Apache incubator-xtable manages storage of large analytical

datasets on DFS (Cloud stores, HDFS or any Hadoop FileSystem compatible storage)

and provides the ability to query them.



This release comes xxx months after xxx. It includes more than
xxx resolved issues, comprising of a few new features as well as
general improvements and bug-fixes. It includes support for
xxx, xxx, xxx, and many more bug fixes and improvements.

For details on how to use incubator-xtable, please look at the quick start page located at https://xtable.apache.org/docs/quick-start-guide.html

If you'd like to download the source release, you can find it here:

https://github.com/apache/incubator-xtable/releases/tag/<VERSION>

You can read more about the release (including release notes) here:

We welcome your help and feedback. For more information on how to
report problems, and to get involved, visit the project website at:

https://xtable.apache.org/

Thanks to everyone involved!

XXX
```

## Recordkeeping

Use [reporter.apache.org](http://reporter.apache.org/) to seed the information about the release into future project reports.

## Social media

Tweet, post on Facebook, LinkedIn, and other platforms. Ask other contributors to do the same.

# Improve the process

It is important that we improve the release processes over time. Once you’ve finished the release, please take a step back and look what areas of this process and be improved. Perhaps some part of the process can be simplified. Perhaps parts of this guide can be clarified.

If we have specific ideas, please start a discussion on the dev@ mailing list and/or propose a pull request to update this guide. Thanks!