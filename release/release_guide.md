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

- Install gpg (`brew install gnupg pinentry-mac` on macOS).
- Create an RSA 4096 key with your apache email: `gpg --full-generate-key`.
- Export its fingerprint into an environment variable `SIGNING_KEY`. The release scripts sign with this key, and fall back to
  gpg's default key when it is unset.

```shell
export SIGNING_KEY=<40-character fingerprint from gpg --list-keys you@apache.org>
```

Append the key to the KEYS file in both dist/dev and dist/release. Committing to dist/release needs PPMC membership; ask a
PPMC member if you are not one.

```shell
for area in dev release; do
  svn checkout --depth=files https://dist.apache.org/repos/dist/${area}/incubator/xtable xtable-${area}
  (gpg --list-sigs ${SIGNING_KEY} && gpg --armor --export ${SIGNING_KEY}) >> xtable-${area}/KEYS
  svn commit xtable-${area}/KEYS -m "Add ${SIGNING_KEY} to KEYS"
done
```

Also add the fingerprint to your profile at [id.apache.org](https://id.apache.org/), so the key is published at
`https://people.apache.org/keys/committer/<apache_id>.asc`.

### Publish your public key to a keyserver

Nexus checks for your public key on keyserver.ubuntu.com and keys.openpgp.org when it closes a staging repository.
pgp.mit.edu is no longer reliable.

```shell
gpg --keyserver hkps://keyserver.ubuntu.com --send-keys ${SIGNING_KEY}
gpg --keyserver hkps://keys.openpgp.org --send-keys ${SIGNING_KEY} # then confirm the emailed link
```

If `--send-keys` fails with a network error, upload the armored key through the web forms at
[keyserver.ubuntu.com](https://keyserver.ubuntu.com/) and [keys.openpgp.org](https://keys.openpgp.org/upload).

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

- Script: [cut_release_branch.sh](https://github.com/apache/incubator-xtable/blob/main/release/scripts/cut_release_branch.sh)

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

## Verify that a Release Build Works

Run "./mvnw -Prelease clean install" to ensure that the build processes are in good shape. // You need to execute this command once you have the release branch in apache/incubator-xtable

```shell
./mvnw -Prelease clean install
```

# Build a release candidate

The core of the release process is the build-vote-fix cycle. Each cycle produces one release candidate. The Release Manager repeats this cycle until the community approves one release candidate, which is then finalized.

Set up a few environment variables to simplify Maven commands that follow. This identifies the release candidate being built. Start with RC_NUM equal to 1 and increment it for each candidate.

1. Generate Source Release: This will create the tarball under incubator-xtable/src_release directory
    1. git checkout ${RELEASE_BRANCH}
    2. cd release && ./scripts/create_source_release.sh
       > The tarball is signed with `SIGNING_KEY` (see One-time Setup), or with gpg's default key if it is unset.
       > The script needs `RC_NUM` exported and checks out the `release-${RELEASE_VERSION}-rc${RC_NUM}` tag.
    3. Verify Source release is signed and buildable
        1. cd ../src_release
        2. gpg --verify apache-xtable-${RELEASE_VERSION}.src.tgz.asc apache-xtable-${RELEASE_VERSION}.src.tgz
        3. git clone git@github.com:apache/incubator-xtable.git incubator-xtable-tmp-clone && cd incubator-xtable-tmp-clone && git checkout ${RELEASE_BRANCH} && cd ..
        4. tar -zxvf apache-xtable-${RELEASE_VERSION}.src.tgz 
        5. diff -qr apache-xtable-${RELEASE_VERSION} incubator-xtable-tmp-clone
        6. Ensure the diff contains only .git, .github, .gitignore, assets, demo, website, rfc, style/IDE.png and xtable-service/examples, all of them only in incubator-xtable-tmp-clone.
        7. cd apache-xtable-${RELEASE_VERSION} && ./mvnw clean package -DskipTests
        8. If they pass, delete the repository we got from the tar-ball
          - cd ../ && rm -rf apache-xtable-${RELEASE_VERSION} && rm -rf incubator-xtable-tmp-clone

2. Create tag
    1. git tag -s release-${RELEASE_VERSION}-rc${RC_NUM} -m "${RELEASE_VERSION}".
       > If you run into some issues, and if want to re-run the same candidate again from start, ensure you delete existing tags before retrying again.
       >
       > // to remove local
       >
       > git tag -d release-${RELEASE_VERSION}-rc${RC_NUM}
       >
       > // to remove remote
       >
       > git push --delete origin release-${RELEASE_VERSION}-rc${RC_NUM}
    2. if apache repo is origin.
       > git push origin release-${RELEASE_VERSION}-rc${RC_NUM}
       >
       > If a branch with the same name already exists in origin, this command might fail as below.
       >
       > error: src refspec release-0.5.3 matches more than one
       >
       > error: failed to push some refs to 'https://github.com/apache/incubator-xtable.git'
       >
       > In such a case, try below command
       >
       > git push origin refs/tags/release-${RELEASE_VERSION}-rc${RC_NUM}

3. [Stage source releases](https://www.apache.org/legal/release-policy.html#stage) on [dist.apache.org](http://dist.apache.org/)
    1. If you have not already, check out the xtable section of the dev repository on [dist.apache.org](http://dist.apache.org/) via Subversion. In a fresh directory
    2. if you would not checkout, please try svn checkout [https://dist.apache.org/repos/dist/dev/incubator/xtable](https://dist.apache.org/repos/dist/dev/incubator/xtable) again.
        1. svn checkout [https://dist.apache.org/repos/dist/dev/incubator/xtable](https://dist.apache.org/repos/dist/dev/incubator/xtable) --depth=immediates
    3. Make a directory for the new release:
        1. mkdir xtable/${RELEASE_VERSION}-rc${RC_NUM}
    4. Copy incubator-xtable source distributions, hashes, and GPG signature:
        1. mv <incubator-xtable-dir>/src_release/* xtable/${RELEASE_VERSION}-rc${RC_NUM}
    5. Add and commit all the files.
        1. cd xtable
        2. svn add ${RELEASE_VERSION}-rc${RC_NUM}
        3. svn --username=${apache_username} --password=${passowrd} commit -m "Add xtable-${RELEASE_VERSION}-rc${RC_NUM}"
    6. Verify that files are [present](https://dist.apache.org/repos/dist/dev/incubator/xtable)
    7. Run Verification Script to ensure the source release is sane
        1. For RC: cd release/scripts && ./validate_staged_release.sh --release=${RELEASE_VERSION} --rc_num=${RC_NUM} --verbose
        2. For finalized release in dev: cd release/scripts && ./validate_staged_release.sh --release=${RELEASE_VERSION} --verbose

4. Deploy maven artifacts and verify
    1. This will deploy jar artifacts to the Apache Nexus Repository, which is the staging area for deploying jars to Maven Central.
    2. Review all staged artifacts (https://repository.apache.org/). They should contain all relevant parts for each module, including pom.xml, jar, test jar, source, test source, javadoc, etc. Carefully review any new artifacts.
    3. git checkout ${RELEASE_BRANCH} && export GPG_TTY=$(tty)
    4. ./release/scripts/deploy_staging_jars.sh
        1. The script signs with `SIGNING_KEY`, the same key as the source release, and disables the Maven build cache, which otherwise fails the deploy with "duplicated artifacts attached".
        2. make sure your IP is not changing while uploading, otherwise it creates a different staging repo
        3. Use a VPN if you can't prevent your IP from switching
    5. Review all staged artifacts by logging into Apache Nexus and clicking on "Staging Repositories" link on left pane. Then find a "open" entry for apachextable
    6. Once you have ensured everything is good and the validation in step 3.7 succeeds, you can close the staging repo. Until you close, you can re-run deploying to staging multiple times. But once closed, it will create a new staging repo. So ensure you close this, so that the next RC (if need be) is on a new repo. So, once everything is good, close the staging repository on Apache Nexus. When prompted for a description, enter
       > Apache incubator-xtable, version `${RELEASE_VERSION}`, release candidate `${RC_NUM}`.
    7. After closing, run the script to validate the staged bundles again:
       ```shell
       ./release/scripts/validate_staged_bundles.sh orgapachextable-<stage_repo_number> ${RELEASE_VERSION}-rc${RC_NUM} 2>&1 | tee -a /tmp/validate_staged_bundles_output.txt
       ```

## Checklist to proceed to the next step

1. Maven artifacts deployed to the staging repository of repository.apache.org
2. Source distribution deployed to the dev repository of dist.apache.org and validated

# Vote on the release candidate

As an incubating project, a release needs two votes: first the XTable PPMC on dev@, then the Incubator PMC (IPMC) on
general@incubator.apache.org. Only IPMC votes are binding for the release. See the
[Incubator release management guide](https://incubator.apache.org/guides/releasemanagement.html#releasing-podling-releases).

## Vote on dev@

Once you have built and individually reviewed the release candidate, start the vote thread on dev@. The release manager
votes too, and every +1 should list what the voter checked. Here's an email template; please adjust as you see fit.

```
To: dev@xtable.apache.org

Subject: [VOTE] Release Apache XTable (incubating) ${RELEASE_VERSION}, release candidate #${RC_NUM}

Hi everyone,

Please review and vote on release candidate #${RC_NUM} for Apache XTable (incubating) ${RELEASE_VERSION}.

[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)

The release candidate includes:

* GitHub release notes [1]
* the source release to be deployed to dist.apache.org [2] (svn revision NNNNN), signed with the key with
  fingerprint ${SIGNING_KEY} [3]
* the sha512 of apache-xtable-${RELEASE_VERSION}.src.tgz: <sha512>
* all convenience binaries to be deployed to the Maven Central Repository [4]
* the source code tag release-${RELEASE_VERSION}-rc${RC_NUM} [5], at commit <commit sha>

The source release builds with Java 11: ./mvnw clean package -DskipTests

The xtable-utilities bundled jar is a local convenience build and is not part of this release. It includes jol-core
(GPL-2.0 with Classpath Exception) and is not deployed to Maven Central.

The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PPMC affirmative votes.
If it passes, the release goes to a vote on general@incubator.apache.org.

Thanks,
Release Manager

[1] https://github.com/apache/incubator-xtable/releases/tag/release-${RELEASE_VERSION}-rc${RC_NUM}
[2] https://dist.apache.org/repos/dist/dev/incubator/xtable/${RELEASE_VERSION}-rc${RC_NUM}/
[3] https://dist.apache.org/repos/dist/release/incubator/xtable/KEYS
[4] https://repository.apache.org/content/repositories/orgapachextable-NNNN/
[5] https://github.com/apache/incubator-xtable/tree/release-${RELEASE_VERSION}-rc${RC_NUM}
```

If there are any issues found in the release candidate, cancel the vote in a separate `[CANCEL] [VOTE] ...` email; there's
no need to wait 72 hours. Proceed to the Fix Issues step below and address the problem. However, some issues don't require
cancellation. For example, if an issue is found in the website, just correct it on the spot and the vote can continue as-is.

If there are no issues, reply on the vote thread to close the voting. Then, tally the votes in a separate email. Here's an
email template; please adjust as you see fit.

```
To: dev@xtable.apache.org

Subject: [RESULT] [VOTE] Release Apache XTable (incubating) ${RELEASE_VERSION}, release candidate #${RC_NUM}

The vote has passed with XXX +1 votes, XXX of which are binding (PPMC):

* approver 1 (binding)
* approver 2 (binding)
* approver 3 (binding)

There are no 0 or -1 votes.

Vote thread: <lists.apache.org link>

The release will now be put to a vote on general@incubator.apache.org. Thanks everyone!
```

## Vote on general@incubator

Start a vote with the same content on general@incubator.apache.org, adding a link to the dev@ vote and result threads.
Votes from mentors on dev@ are IPMC votes and can be carried over; say so in the email.

```
To: general@incubator.apache.org

Subject: [VOTE] Release Apache XTable (incubating) ${RELEASE_VERSION}, release candidate #${RC_NUM}

Hello Incubator PMC,

The Apache XTable community has voted and approved release candidate #${RC_NUM} of Apache XTable (incubating)
${RELEASE_VERSION}. We now kindly request that the IPMC vote on this release.

The XTable community vote thread: <link>
The vote result: <link>
Carried-over binding IPMC votes from the dev@ vote: <names>

<the release candidate details from the dev@ email>

The vote will be open for at least 72 hours. It needs at least 3 binding IPMC +1 votes and more +1 than -1 votes.
```

Close it with a `[RESULT] [VOTE]` email to general@ in the same format as the dev@ one, listing binding IPMC votes.

## Checklist to proceed to the finalization step

1. The dev@ vote passed with at least three approving PPMC votes.
2. The general@ vote passed with at least three binding IPMC votes.

# Fix any issues

Any issues identified during the community review and vote should be fixed in this step.

Code changes should be proposed as standard pull requests to the master branch and reviewed using the normal contributing process. Then, relevant changes should be cherry-picked into the release branch. The cherry-pick commits should then be proposed as the pull requests against the release branch, again reviewed and merged using the normal contributing process.

Once all issues have been resolved, you should go back and build a new release candidate with these changes.

## Checklist to proceed to the next step

Issues identified during vote have been resolved, with fixes committed to the release branch.

# Finalize the release

Once the release candidate has been reviewed and approved by the community, the release should be finalized. This involves the final deployment of the release candidate to the release repositories, merging of the website changes, etc.

## Current follow the below steps to finalize the release.

Do not start until both the dev@ and the general@ votes have passed.

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
    5. Verify that files are [present](https://dist.apache.org/repos/dist/release/incubator/xtable)
    6. Remove the previous release from dist/release; it stays available on [archive.apache.org](https://archive.apache.org/dist/incubator/xtable/).
       Point its download links in `website/releases/downloads.mdx` at the archive.
       ```shell
       svn rm xtable/<PREVIOUS_VERSION>
       svn commit -m "Remove xtable <PREVIOUS_VERSION>, now in the archive"
       ```
    7. Delete the RC directories from https://dist.apache.org/repos/dist/dev/incubator/xtable.
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

Subject: [ANNOUNCE] Apache XTable (incubating) <VERSION> released

The Apache XTable (incubating) team is pleased to announce the release of Apache XTable (incubating) <VERSION>.

Apache XTable (incubating) is a cross-table converter for table formats such as Apache Hudi, Apache Iceberg and
Delta Lake, which lets you write data in one format and read it in another.



This release comes xxx months after xxx. It includes more than
xxx resolved issues, comprising of a few new features as well as
general improvements and bug-fixes. It includes support for
xxx, xxx, xxx, and many more bug fixes and improvements.

For details on how to use incubator-xtable, please look at the quick start page located at https://xtable.apache.org/docs/quick-start-guide.html

If you'd like to download the source release, you can find it here:

https://xtable.apache.org/releases/downloads

You can read more about the release (including release notes) here:

We welcome your help and feedback. For more information on how to
report problems, and to get involved, visit the project website at:

https://xtable.apache.org/

Thanks to everyone involved!

XXX

----
Apache XTable (incubating) is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the
Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that
the infrastructure, communications, and decision making process have stabilized in a manner consistent with
other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or
stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
```

## Recordkeeping

Use [reporter.apache.org](http://reporter.apache.org/) to seed the information about the release into future project reports.

## Social media

Tweet, post on Facebook, LinkedIn, and other platforms. Ask other contributors to do the same.

# Improve the process

It is important that we improve the release processes over time. Once you’ve finished the release, please take a step back and look what areas of this process and be improved. Perhaps some part of the process can be simplified. Perhaps parts of this guide can be clarified.

If we have specific ideas, please start a discussion on the dev@ mailing list and/or propose a pull request to update this guide. Thanks!