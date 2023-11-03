# Installation

This page covers the essential steps to setup OneTable in your environment.

## Pre-requisites
1. Building the project requires Java 11 and Maven to be setup and configured using PATH or environment variables. 
2. Clone the OneTable project github [repository](https://github.com/onetable-io/onetable) in your environment.

## Steps
#### Building the project 
Once the project is successfully cloned in your environment, you can build the project using the below command.

```shell md title=="shell"
mvn clean package
```

#### Install the package 
Once the code is compiled, you can install the jar locally.

```shell md title=="shell"
mvn clean install
```

For more information on the steps, follow the project's github [README.md](https://github.com/onetable-io/onetable/blob/main/README.md) 

## Next Steps
See the [Quickstart](https://onetable.dev/docs/how-to) guide to learn to use OneTable to add interoperability between
different table formats.