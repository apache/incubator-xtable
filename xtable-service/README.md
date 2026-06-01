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

# XTable REST Service

The `rest-service-open-api.yaml` defines the api contract for running table format conversion using XTable's REST service. 
See XTable's `spec` module for more details: https://github.com/apache/incubator-xtable/tree/main/spec

## How to run the service locally

#### Before running the service, ensure that you have the required credentials set in your enviroment needed to read and write to cloud storage.

To run the service locally, first ensure you have built the project with 
```sh
./mvnw clean install -DskipTests
```


Then you can run start the quarkus service using the following command:
```sh
./mvnw quarkus:dev -pl xtable-service
```
This will start the service on `http://localhost:8080`.

Note quarkus will automatically reload the service when you make changes to the code.

## Testing with Postman

If you would like to test the service with an api client, you can download Postman https://www.postman.com/downloads/

Ensure that when you are testing that you have set the service URL, headers, and request body correctly. 
See the screenshots below for an example. 

![Screenshot 2025-05-01 at 9.04.59 AM.png](examples/Screenshot%202025-05-01%20at%209.04.59%E2%80%AFAM.png)

![Screenshot 2025-05-01 at 9.05.10 AM.png](examples/Screenshot%202025-05-01%20at%209.05.10%E2%80%AFAM.png)