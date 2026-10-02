<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Open API spec

The `rest-service-open-api.yaml` defines the api contract for running table format conversion using XTable's REST service.
This spec is still under active development and is subject to changes.

## Lint

To make sure that the open-api definition is valid, you can run the `lint` command:

This will first run `make install`, which in turn will create the venv (if needed) and install openapi-spec-validator
```sh
make lint
```

if you want to wipe out the venv and start fresh run make clean
```sh
make clean
```

## Generate 

Note: You’ll need the OpenAPI Generator CLI installed and on your `PATH`. You can install it using Homebrew:
```sh
brew install openapi-generator
```
Then to generate the java models from the open-api spec, you can run the `generate-models` command.

```sh
make generate-models
```
If you would to remove the generated models, you can run the `clean-models` command:
```sh
make clean-models
```