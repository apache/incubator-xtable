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

# RFC-[4]: XCatalogSync - Synchronize access control policies across catalogs

## Proposers

- @vinishjail97

## Approvers

- Anyone from XTable community can approve/add feedback.

## Status

GH Feature Request: <link to umbrella JIRA>

> Please keep the status updated in `rfc/README.md`.

## Abstract

Today, numerous catalogs have emerged, each with its own specifications for table creation, metadata refreshing, and implementing data governance rules. 
This diversity has led to increased complexity and confusion, making it challenging for users to choose the right catalog. To address this challenge, we previously proposed an [RFC](https://github.com/apache/incubator-xtable/pull/605/files) for synchronizing table format metadata across catalogs.  

In this RFC, we extend that vision to focus on synchronizing data governance policies. The aim is to enable policies defined in a source catalog to be seamlessly synchronized with multiple target catalogs. This approach not only simplifies multi-catalog operations but also fosters consistency and reduces the manual effort required to manage governance across a fragmented ecosystem.  

## Motivation 
A recent blog post in [Data Engineering Weekly](https://www.dataengineeringweekly.com/p/the-chaos-of-catalogs) highlighted the challenges of managing metadata and data governance in a fragmented ecosystem of catalogs. It emphasized the need for scalable solutions, such as adopting a federated catalog model, to address the operational friction caused by the coexistence of multiple catalogs.

## Background
An access control policy defines a rule stating, "A principal has specific privileges for a securable object." In the context of data catalogs, these privileges can include actions like SELECT or CREATE statements used in DDL, DML, or DQL queries, the securable objects can range from databases and tables to columns and beyond. When a catalog is connected to a query engine, it enforces these permissions for the principal (user) either directly or by issuing temporary credentials that the query engine can use to execute queries securely.  

In today’s data ecosystem, numerous catalogs exist, each with its own specifications and methods for enforcing access control policies. Some catalogs, like AWS Glue and BigLake, are tightly integrated within their ecosystems, while others rely on credential-sharing approaches to support multiple query engines. Similar to how we have defined [InternalTable](https://github.com/apache/incubator-xtable/blob/main/xtable-api/src/main/java/org/apache/xtable/model/InternalTable.java), we aim to establish a canonical representation for access control policies and synchronize these policies across different catalogs

## Implementation
After reviewing the specifications of multiple catalogs (HMS, AWS Glue LakeFormation, Unity, Polaris, etc.), we observed that most follow a similar conceptual model for access control, incorporating roles, users, user-groups, privileges, and securable objects. While there are slight variations in naming and nuances, these foundational concepts align closely with the design principles originally established by HMS.

For example, HMS defines enums for [PrivilegeType](https://learn.microsoft.com/en-us/azure/databricks/data-governance/table-acls/object-privileges#privilege-types) (SELECT, CREATE, MODIFY, USAGE, READ_METADATA, CREATE_NAMED_FUNCTION, MODIFY_CLASSPATH, ALL PRIVILEGES) and [SecurableObjectType](https://learn.microsoft.com/en-us/azure/databricks/data-governance/table-acls/object-privileges#securable-objects) (CATALOG, SCHEMA, TABLE, VIEW, FUNCTION) which form the basis for hierarchical asset management like Catalog ➝ Schema ➝ Table. Many other catalogs, such as Glue LakeFormation, Polaris, and Unity, extend or adapt this approach to fit their specific ecosystems, often expanding the scope of securable objects. Interestingly, these catalogs also share similar [authorization APIs](https://github.com/apache/hive/blame/master/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift#L3051) originally derived from HMS's thrift spec, enabling operations like granting and listing permissions.  

Below is the first version of the models we will be using internally that allows us to interoperate and synchronize across multiple catalogs, it's not the final one, and we can improve it as we add implementations for source and target catalogs.

**InternalPrivilege**
```
/**
 * Represents a single privilege assignment for a securable object.
 *
 * <p>This defines the kind of operation (e.g., SELECT, CREATE, MODIFY) and whether it is allowed or
 * denied. Some catalogs may only accept ALLOW rules and treat all other operations as denied by
 * default.
 */
public class InternalPrivilege {
  /**
   * The type of privilege, such as SELECT, CREATE, or MODIFY. Each implementation can define its
   * own set of enums.
   */
  InternalPrivilegeType privilegeType;

  /**
   * The decision, typically ALLOW or DENY. Some catalogs may not support DENY explicitly,
   * defaulting to ALLOW.
   */
  String privilegeDecision;
}
```

**InternalSecurableObject**
```
/**
 * Represents a securable object in the catalog, which can be managed by access control.
 *
 * <p>Examples of securable objects include catalogs, schemas, tables, views, or any other data
 * objects that require fine-grained privilege management. Each securable object can have one or
 * more privileges assigned to it.
 */
public class InternalSecurableObject {
  /** The identifier of the securable object. */
  InternalSecurableObjectIdentifier securableObjectIdentifier;
  /**
   * The type of securable object, such as TABLE, VIEW, FUNCTION, etc. Each implementation can
   * define its own set of enums.
   */
  InternalSecurableObjectType securableObjectType;
  /** The set of privileges assigned to this object. */
  List<InternalPrivilege> privileges;
}
```

**InternalChangeLogInfo**
```
/**
 * Contains change-log information for roles, users, or user groups, enabling traceability of who
 * created or last modified them.
 *
 * <p>This class is useful for governance and compliance scenarios, where an audit trail is
 * necessary. It can be extended to include additional fields such as reasonForChange or
 * changeDescription.
 */
public class InternalChangeLogInfo {
  /** The username or identifier of the entity that created this record. */
  String createdBy;

  /** The username or identifier of the entity that last modified this record. */
  String lastModifiedBy;

  /** The timestamp when this record was created. */
  Instant createdAt;

  /** The timestamp when this record was last modified. */
  Instant lastModifiedAt;
}
```


**InternalRole**
```
/**
 * Represents a role within the catalog.
 *
 * <p>A role can be granted access to multiple securable objects, each with its own set of
 * privileges. Audit info is stored to track the role's creation and modifications, and a properties
 * map can hold additional metadata.
 */
public class InternalRole {
  /** The unique name or identifier for the role. */
  String name;

  /** The list of securable objects this role can access. */
  List<InternalSecurableObject> securableObjects;

  /** Contains information about how and when this role was created and last modified. */
  InternalChangeLogInfo changeLogInfo;

  /**
   * A map to store additional metadata or properties related to this role. For example, this might
   * include a description, usage instructions, or any catalog-specific fields.
   */
  Map<String, String> properties;
}

```

**InternalUser**
```
/**
 * Represents an individual user within the catalog.
 *
 * <p>A user may be assigned multiple roles, and can also belong to a specific user group. Audit
 * information is stored to allow tracking of who created or last modified the user.
 */
public class InternalUser {
  /** The unique name or identifier for the user. */
  String name;

  /** The list of roles assigned to this user. */
  List<InternalRole> roles;

  /** Contains information about how and when this user was created and last modified. */
  InternalChangeLogInfo changeLogInfo;
}
```

**InternalUserGroup**
```
/**
 * Represents a user group within the catalog.
 *
 * <p>Groups can have multiple roles assigned, and also include audit information to track creation
 * and modifications.
 */
public class InternalUserGroup {
  /** The unique name or identifier for the user group. */
  String name;

  /** The list of roles assigned to this group. */
  List<InternalRole> roles;

  /** Contains information about how and when this group was created and last modified. */
  InternalChangeLogInfo changeLogInfo;
}
```

**InternalAccessControlPolicySnapshot**
```
/** A snapshot of all access control data at a given point in time. */
public class InternalAccessControlPolicySnapshot {
  /**
   * A unique identifier representing this snapshot's version.
   *
   * <p>This could be a UUID, timestamp string, or any value that guarantees uniqueness across
   * snapshots.
   */
  String versionId;

  /**
   * The moment in time when this snapshot was created.
   *
   * <p>Useful for maintaining an audit trail or comparing how policies have changed over time.
   */
  Instant timestamp;

  /**
   * A map of user names to {@link InternalUser} objects, capturing individual users' details such
   * as assigned roles, auditing metadata, etc.
   */
  @Builder.Default Map<String, InternalUser> usersByName = Collections.emptyMap();

  /**
   * A map of group names to {@link InternalUserGroup} objects, representing logical groupings of
   * users for easier role management.
   */
  @Builder.Default Map<String, InternalUserGroup> groupsByName = Collections.emptyMap();

  /**
   * A map of role names to {@link InternalRole} objects, defining the privileges and security rules
   * each role entails.
   */
  @Builder.Default Map<String, InternalRole> rolesByName = Collections.emptyMap();

  /**
   * A map of additional properties or metadata related to this snapshot. This map provides
   * flexibility for storing information without modifying the main schema of the snapshot.
   */
  @Builder.Default Map<String, String> properties = Collections.emptyMap();
}
```

A new interface `CatalogAccessControlPolicySyncClient` will be used for converting the catalogs' policy definitions to the internal model and vice versa.  

```
/**
 * Defines the contract for synchronizing access control policies between a specific catalog and the
 * internal canonical model.
 *
 * <p>Implementations of this interface are responsible for:
 *
 * <ul>
 *   <li>Fetching the catalog’s native policy definitions and converting them into the canonical
 *       model.
 *   <li>Converting the canonical model back into the catalog’s format and updating the catalog
 *       accordingly.
 * </ul>
 */
public interface CatalogAccessControlPolicySyncClient {
  /**
   * Fetches the current policies from the catalog, converting them into the internal canonical
   * model.
   *
   * <p>This method allows you to pull in the catalog’s native policy definitions (e.g., roles,
   * privileges, user/groups) and map them into a {@link InternalAccessControlPolicySnapshot} so
   * that they can be managed or merged with your centralized policy framework.
   *
   * @return A {@code CatalogAccessControlPolicySnapshot} containing the catalog’s current policies.
   */
  InternalAccessControlPolicySnapshot fetchPolicies();

  /**
   * Pushes the canonical policy snapshot into the target catalog, converting it into the catalog’s
   * native policy definitions and applying any necessary updates.
   *
   * <p>This method typically performs the following steps:
   *
   * <ol>
   *   <li>Transforms the given {@code InternalAccessControlPolicySnapshot} into the catalog’s
   *       native format (roles, privileges, etc.).
   *   <li>Applies the resulting policy definitions to the catalog, potentially overwriting or
   *       merging existing policies.
   *   <li>Returns a {@link SyncResult} detailing the success or failure of the operation.
   * </ol>
   *
   * @param snapshot The access control policy snapshot to be synchronized with the catalog.
   */
  void pushPolicies(InternalAccessControlPolicySnapshot snapshot);
}
```


## Rollout/Adoption Plan

- Are there any breaking changes as part of this new feature/functionality? 
  - None, this is a new functionality providing access control policy synchronization across catalogs.     
- What impact (if any) will there be on existing users?
  - N/A.
- If we are changing behavior how will we phase out the older behavior? When will we remove the existing behavior?
  - N/A
- If we need special migration tools, describe them here.
  - N/A

## Test Plan

Based on community feedback, we will determine the initial set of catalogs to support. Two-way policy synchronization will then be validated for these catalogs to ensure functionality and reliability.