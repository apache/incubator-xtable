#!/bin/bash
# Create the require jars for the demo and copy them into a directory we'll mount in our notebook container
cd .. && mvn install -am -pl core -DskipTests -T 2
cp hudi-support/utils/target/hudi-utils-0.1.0-SNAPSHOT.jar demo/jars
cp api/target/onetable-api-0.1.0-SNAPSHOT.jar demo/jars
cp core/target/onetable-core-0.1.0-SNAPSHOT.jar demo/jars

cd demo
docker-compose up