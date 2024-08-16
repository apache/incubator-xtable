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

## Verify that a Release Build Works

Run "mvn -Prelease clean install" to ensure that the build processes are in good shape. // You need to execute this command once you have the release branch in apache/incubator-xtable

```shell
mvn -Prelease clean install
```

# Build a release candidate

The core of the release process is the build-vote-fix cycle. Each cycle produces one release candidate. The Release Manager repeats this cycle until the community approves one release candidate, which is then finalized.

Set up a few environment variables to simplify Maven commands that follow. This identifies the release candidate being built. Start with RC_NUM equal to 1 and increment it for each candidate.

1. Generate Source Release: This will create the tarball under incubator-xtable/src_release directory
    1. git checkout ${RELEASE_BRANCH}
    2. cd release && ./scripts/create_source_release.sh
       > If you have multiple gpg keys(gpg --list-keys), then the signing command will take in the first key most likely.
       > You will release this when it asks for a passphrase in a pop up.
       > When asked for passphrase, ensure the intended key is the one asked for.
       >
       > Command used in script:
       >
       > gpg --armor --detach-sig ${RELEASE_DIR}/incubator-xtable-${RELEASE_VERSION}.src.tgz
       >
       > To use a specific key: update as follows: // replace FINGERPRINT
       >
       > gpg --local-user [FINGERPRINT] --armor --detach-sig ${RELEASE_DIR}/incubator-xtable-${RELEASE_VERSION}.src.tgz
    3. Verify Source release is signed and buildable
        1. cd ../src_release
        2. gpg --verify apache-xtable-${RELEASE_VERSION}.src.tgz.asc apache-xtable-${RELEASE_VERSION}.src.tgz
        3. git clone git@github.com:apache/incubator-xtable.git incubator-xtable-tmp-clone && cd incubator-xtable-tmp-clone && git checkout ${RELEASE_BRANCH} && cd ..
        4. tar -zxvf apache-xtable-${RELEASE_VERSION}.src.tgz 
        5. diff -qr apache-xtable-${RELEASE_VERSION} incubator-xtable-tmp-clone
        6. Ensure the diff contains assets,demo,website and non-binary files only in incubator-xtable-tmp-clone.
        7. cd apache-xtable-${RELEASE_VERSION} && mvn clean package -DskipTests
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
        2. For finalized release in dev: cd scripts && ./scripts/validate_staged_release.sh --release=${RELEASE_VERSION}  --verbose

4. Deploy maven artifacts and verify
    1. This will deploy jar artifacts to the Apache Nexus Repository, which is the staging area for deploying jars to Maven Central.
    2. Review all staged artifacts (https://repository.apache.org/). They should contain all relevant parts for each module, including pom.xml, jar, test jar, source, test source, javadoc, etc. Carefully review any new artifacts.
    3. git checkout ${RELEASE_BRANCH} && export GPG_TTY=$(tty)
    4. mvn deploy -Prelease -DskipTests  -DdeployArtifacts=true
        1. when prompted for the passphrase, if you have multiple gpg keys in your keyring, make sure that you enter the right passphase corresponding to the same key (FINGERPRINT) as used while generating source release in step f.ii.
           > If the prompt is not for the same key (by default the maven-gpg-plugin will pick up the first key in your keyring so that could be different), then add the following option to your ~/.gnupg/gpg.conf file
        2. make sure your IP is not changing while uploading, otherwise it creates a different staging repo
        3. Use a VPN if you can't prevent your IP from switching
    5. Review all staged artifacts by logging into Apache Nexus and clicking on "Staging Repositories" link on left pane. Then find a "open" entry for apachextable
    6. Once you have ensured everything is good and validation of step 7 succeeds, you can close the staging repo. Until you close, you can re-run deploying to staging multiple times. But once closed, it will create a new staging repo. So ensure you close this, so that the next RC (if need be) is on a new repo. So, once everything is good, close the staging repository on Apache Nexus. When prompted for a description, enter
       > Apache incubator-xtable, version `${RELEASE_VERSION}`, release candidate `${RC_NUM}`.
    7. After closing, run the script to validate the staged bundles again:
       ```shell
       ./release/scripts/validate_staged_bundles.sh orgapachextable-<stage_repo_number> ${RELEASE_VERSION}-rc${RC_NUM} 2>&1 | tee -a /tmp/validate_staged_bundles_output.txt
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
6. Finalize the Release in GH by providing the release date.
7. Update [DOAP]

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