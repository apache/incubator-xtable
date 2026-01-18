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
# RFC-2: Module Structure

## Proposers

- @the-other-tim-brown

## Approvers
- @<approver1 github username>
- @<approver2 github username>

## Status

GH Feature Request: https://github.com/apache/incubator-xtable/issues/611

> Please keep the status updated in `rfc/README.md`.

## Abstract
This proposal aims to make it easier for users to run the XTable code when incorporating it into their own project or leveraging the pre-built jars published by the XTable project.
As part of this, there is a secondary goal to make sure that the jars published, including the bundled jars, are trimmed down to the minimum size possible to prevent bloat for our users and to make sure there are no compliance issues with the dependencies. 

## Background
Currently, XTable users have two options for running the conversion code.
They can create their own jar with the XTable dependencies specified in their build file or they can generate the xtable-utilities bundled jar and run that on its own.
There are difficulties when using either approach currently.

When running the prebuilt utilities bundle jar, the user will bring in all the required dependencies which currently include three table formats, spark, and hadoop. 
This results in a very large jar containing more than the user really needs.

When building your own jar, you can run into issues with conflicts in versions for the specific table formats you want to use since the user will often already have an implementation of at least one format on their classpath. 
These conflicts may only surface as runtime errors when there is difference in method signature for example. 
This can make the experience brittle for the end user by requiring them to closely follow any upgrades within the XTable repo as well.
In this scenario the user will also have dependencies on all 3 table formats by default.

## Implementation
In order to allow users to include only the dependencies that they need for their use case, the repo will need to have modules per table format and per catalog client in the future.
This will allow the user to include only the relevant modules at runtime to fit their needs. Furthermore, each of these sub-modules will publish a shaded jar that shades the format specific dependencies. 
This will allow the user to easily add jars and the required dependencies when executing the standalone RunSync command as well as avoid issues with version differences when adding these jars to an existing class-path that may contain table format related dependencies already.
In addition to this, the jars will still be published to allow users to compose their own shaded jar if they have specific needs or customizations that they want.

We can also create these modules with a suffix indicating which version of the format is supported to allow for different implementations or versions of dependencies for the same format. For example, have an xtable-iceberg-v2 and xtable-iceberg-v3 module. 

![img.png](assets/images/xtable_dependencies.jpg)

## Rollout/Adoption Plan

- Are there any breaking changes as part of this new feature/functionality?
Yes, users will need to add new jars to their class-paths when running commands. Additionally, users may need to import different modules into their builds if they are constructing their own jar.
- What impact (if any) will there be on existing users?
The user should be able to use only the dependencies for the formats that they are interested in converting between.
- If we are changing behavior how will we phase out the older behavior? When will we remove the existing behavior?
There will be upgrade instructions included in the next set of Release Notes
- If we need special migration tools, describe them here.
None needed, just need to update documentation.

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing breaks?