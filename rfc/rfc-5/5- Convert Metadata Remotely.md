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

# RFC-5: XTable REST Service - Handle Metadata Conversion Remotely

## Proposers

- Rahil Chertara
- Vaibhav Kumar

## Approvers

- Timothy Brown
- Vinish Reddy

## Abstract

Currently there are users that want to leverage Apache XTable for metadata conversion, but would rather avoid injecting XTable dependencies (and its transitive dependencies from Delta, Apache Iceberg, Apache Hudi, etc.) within their application/services. There can be several reasons, such as some applications/services wanting to keep their packaging footprint smaller, not handle dependency management due to potential conflicting versions of the same dependency, or are not within the Java ecosystem.

For these users, we could create a REST service that's primary responsibility is running XTable and initiating metadata conversion without the need for users to have to manually build and run the xtable-utilities-bundle.jar. In this RFC we will discuss some current use cases for this service, and also some initial design considerations.

## Background

### Data Catalog Integrations

Currently there are many data catalogs on the market such as Databricks Unity, AWS Glue etc that are able to store open table formats like Iceberg, Hudi, Delta. As data catalogs offer more robust features for open table formats such as running  “compaction”, “clustering”, etc, we see “conversion” between table formats as another key service for customers with multi-format use cases.

For example, the Apache Polaris community is interested in representing other table formats out of Apache Iceberg within their catalog service. They have recently merged a specification known as Generic Tables, and shared ideas around a Table Convertor component that can live outside Polaris.There is an interest within Polaris to use XTable as this component, and making a request to an external service. We have made the following proposals in their community here.

We anticipate that other data catalogs would support table format conversion as a “service”, and can leverage our XTable’s REST service rather than having to reinvent the wheel.

### Supporting Non-JVM Clients

By providing a REST service that confirms to an open spec, it becomes feasible for other programming languages outside of Java, such as Python/Rust to be able to interact with Apache XTable for metadata conversion. For these non-jvm clients, there are no XTable dependencies needed, as long as they are able to serialize/deserialize JSON payloads expressing the open api spec required for an XTable conversion. See below for open api spec

## Implementation

### Open API Spec

#### Note this is the initial sketch for the apis, and subject to change/evolve during implementation. We will first aim to land the runSync endpoint and then iterate as needed.

#### Convert Table Endpoint

We can expose an endpoint which will handle metadata conversion inline(meaning the client will wait until our service finishes conversion, and then returns a response). This endpoint effectively would be invoking XTable’s runSync method and will directly be reading paths from an existing table’s metadata in cloud storage.

```yaml
  /v1/conversion/table:
    post:
      tags:
        - XTable Service API
      summary: Initiate XTable's runSync process to convert a source table format to the requested target table formats.
      description: |
        Reads the source table metadata from storage, converts it to the requested
        target formats.
      operationId: convertTable
      parameters:
        - in: header
          name: Prefer
          description: Use 'respond-async' to request asynchronous processing.
          required: false
          schema:
            type: string
            enum:
              - respond-async
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ConvertTableRequest'
      responses:
        '200':
          $ref: '#/components/responses/ConvertTableResponse'
        '202':
          $ref: '#/components/responses/SubmittedConversionResponse'
        '403':
          $ref: '#/components/responses/ForbiddenResponse'
        '503':
          $ref: '#/components/responses/ServiceUnavailableResponse'
        default:
          $ref: '#/components/responses/ErrorResponse'
```

The client will send a ConvertTableRequest which will contain core parameters needed for us to run an XTable sync, similar to what we request users to provide when running the jar here.

```yaml
ConvertTableRequest:
  type: object
  required:
    - source-format
    - source-table-name
    - source-table-path
    - target-formats
  properties:
    source-format:
      type: string
      description: Name of the source table format (e.g., "ICEBERG", "HUDI", "DELTA")
    source-table-name:
      type: string
      description: Name of the source table
    source-table-path:
      type: string
      description: Path to the source table's metadata
    target-formats:
      type: array
      items:
        type: string
      description: List of desired output table formats (e.g., "ICEBERG", "HUDI", "DELTA")
    configurations:
      type: object
      description: Additional configuration key/value pairs (e.g., storage credentials or other service configs)
      additionalProperties:
        type: string
```

The ConvertTableResponse will contain a list of `conversions`, where each object is a `TargetTable`. The `TargetTable` contains the path to the target’s format metadata, name of the target-format, and an optional serialized avro schema string.

```yaml
ConvertTableResponse:
  type: object
  required:
    - conversions
  properties:
    conversions:
      type: array
      description: A list of converted tables, one per requested format
      items:
        $ref: '#/components/schemas/TargetTable'

TargetTable:
  type: object
  required:
    - target-format
    - target-metadata-path
  properties:
    target-format:
      type: string
      description: Name of the target format (e.g., "ICEBERG", "HUDI", "DELTA")
    target-metadata-path:
      type: string
      description: Path where the converted metadata was written
    target-schema:
      type: string
      description: Schema definition of the converted table
```

#### Catalog Sync Endpoint

We can expose an endpoint which will handle running XTable’s runCatalogSync, for users that want to provide XTable with table identifiers rather than storage paths.

```yaml
/v1/conversion/catalog:
    post:
      tags:
        - XTable Service API
      Summary: Initiate XTable's runCatalogSync process
      Description: 

      operationId: runCatalogSync
      requestBody:
        content:
          application/json: 
            schema:
              $ref: '#/components/schemas/RunCatalogSyncRequest'
      responses:
        200:
          $ref: '#/components/responses/RunCatalogSyncResponse'
        403:
          $ref: '#/components/responses/ForbiddenResponse'
        503:
          $ref: '#/components/responses/ServiceUnavailableResponse'
```

The RunCatalogSyncRequest request would follow a similar model as our docs for runCatalog sync.
```yaml
RunCatalogSyncRequest:
  type: object
  required:
    - source-catalog-type
    - source-catalog-id
    - source-table-identifier
    - target-catalog-type
    - target-catalog-id
    - target-table-identifier
    - target-format
  properties:
    source-catalog-type:
      type: string
    source-catalog-id:
      type: string
    source-table-identifier:
      type: string
    target-catalog-type:
      type: string
    target-catalog-id:
      type: string
    target-table-identifier:
      type: string
    target-format:
      type: string
```

The response would contain a result which indicates SUCCESS, FAILED, and an optional error message.

```yaml
RunCatalogSyncResponse:
  type: object
  Required:
    - catalog-sync-result
  Properties:
    catalog-sync-result:
      type: string
    error-message:
      type: string
```

### Service Framework

When looking at several Java REST frameworks, the two most popular frameworks that are currently used in industry are Spring Boot, and Quarkus, see one analysis here. Ideally the framework we use should be able to handle things such as authentication , integration with popular clouds, and has a good developer ecosystem.

**Quarkus**  
Modern data catalogs such as Polaris and Nessie have opted to use Quarkus, due to the framework's strong integration with Kubernetes for deployment. From a performance standpoint, it has a lower memory footprint, and faster startup times than Spring. In addition Quarkus has a good developer experience with its ability to reload fast after local changes are made. The main con is that the framework is still newer, and the community is not as large as a more established framework like Spring and may not have as many integrations or extensions.

**Spring**  
Spring on the other hand has been battle tested for years, and has a plethora of documentation and integrations natively built. Spring contains intuitive annotations, auto-configuration, and excellent tooling within most IDEs. Finally from an ease of use standpoint it would be easier for developers to likely build on this service as many developers would be exposed to Spring due its popularity.

If performance is critical for this service we should opt for Quarkus, however if the XTable community is more familiar with Spring then we should opt for this for ease of use.

### Credential Handling

Currently XTable uses the Hadoop FileSystem API to read underlying metadata within cloud storage such as AWS S3. For example today if a user is doing a sync with their tables on storage, XTable uses the hadoop-aws dependency and provides the following hadoop-defaults.xml which has a property fs.s3.aws.credentials.provider which sets the credential provider as software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.

The issue however is that the default credentials provider is looking for java system props, or environment variables for things such as AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to be set prior to running XTable. If those values are not set, then XTable will be unable to interact with the user’s cloud environment and hit some AccessDeniedException.

Here are a couple of options on how we can proceed.

#### Option 1 (User running the service responsibility):

During the initialization of the service it is the responsibility of the user to configure the above environment variables, so that the service can interact with the user's cloud environment to access storage paths. The con of this approach is that if we have multiple callers to this service with different credentials scoped to different storage buckets, this will not work as these environment variables are static and set once.

#### Option 2 (Caller provides a Role ARN)

Either during initialization of our service, or when the caller is making a request to our service, the caller provides a ROLE_ARN to a role that has the permissions needed to read from storage. It is assumed that the caller that has provided this ROLE_ARN has added our service as a trusted principal(that has the ability to assume this role). Within our service we then follow an STS client, to get credentials similar to here.


## Rollout/Adoption Plan

- Are there any breaking changes as part of this new feature/functionality?
    - There are no breaking changes as we are adding a new module for this service.
- What impact (if any) will there be on existing users?
    - None on existing users, as there will be a new module for building and deploying this service 
- If we are changing behavior how will we phase out the older behavior? When will we remove the existing behavior?
    - N/A
- If we need special migration tools, describe them here.
    - N/A

## Test Plan

We plan to add unit tests, and IT tests with a test client that hits the endpoints exposed by this service.