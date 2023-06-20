#!/bin/bash

if ! mvn clean verify -Dformat.skip=true -DskipTests -DskipITs -pl debezium-bom,debezium-core,debezium-connector-mongodb; then
    echo "########## checkstyle failed ##########"
    echo "Please run the following command to format the files."
    echo "mvn clean verify -DskipTests -DskipITs -pl debezium-bom,debezium-core,debezium-connector-mongodb"
    exit 1
fi
