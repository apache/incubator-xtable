# Installation

This page covers the essential steps to setup Apache XTable™ (incubating) in your environment.

## Pre-requisites
1. Building the project requires Java 11 and Maven to be setup and configured using PATH or environment variables. 
2. Clone the Apache XTable™ (Incubating) project GitHub [repository](https://github.com/apache/incubator-xtable) in your environment.

## Steps
#### Building the project 
Once the project is successfully cloned in your environment, you can build the jars from the source using the below command.

```shell md title=="shell"
mvn clean package
```
For skipping the tests while building, add `-DskipTests`.

```shell md title=="shell"
mvn clean package -DskipTests
```

For more information on the steps, follow the project's GitHub [README.md](https://github.com/apache/incubator-xtable/blob/main/README.md) 

## Next Steps
See the [Quickstart](/docs/how-to) guide to learn to use Apache XTable™ (Incubating) to add interoperability between
different table formats.