#!/bin/bash

DEBEZIUM_REPOSITORY=debezium/debezium
DEBEZIUM_BRANCH=main

SNAPSHOT_VERSION=$(curl -s https://raw.githubusercontent.com/$DEBEZIUM_REPOSITORY/$DEBEZIUM_BRANCH/pom.xml | grep -o '<version>.*-SNAPSHOT</version>' | awk -F '[<>]' '{print $3}')

docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD
docker login -u ${QUAYIO_CREDENTIALS%:*} -p ${QUAYIO_CREDENTIALS#*:} quay.io
docker build --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t debezium/connect:nightly connect/snapshot
docker push debezium/connect:nightly
docker tag debezium/connect:nightly quay.io/debezium/connect:nightly
docker push quay.io/debezium/connect:nightly
