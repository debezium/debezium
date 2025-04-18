#!/bin/bash

DEBEZIUM_REPOSITORY=debezium/debezium
DEBEZIUM_BRANCH=main

SNAPSHOT_VERSION=$(curl -s https://raw.githubusercontent.com/$DEBEZIUM_REPOSITORY/$DEBEZIUM_BRANCH/pom.xml | grep -o '<version>.*-SNAPSHOT</version>' | awk -F '[<>]' '{print $3}')

./setup-local-builder.sh
docker run --privileged --rm mirror.gcr.io/tonistiigi/binfmt --install all

docker login -u ${QUAYIO_CREDENTIALS%:*} -p ${QUAYIO_CREDENTIALS#*:} quay.io

# connect
docker buildx build --push --platform "${DEFAULT_PLATFORMS}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/connect:nightly connect/snapshot

# server
docker buildx build --push --platform "${DEFAULT_PLATFORMS}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/server:nightly server/snapshot

# operator
docker buildx build --push --platform "${DEFAULT_PLATFORMS}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/operator:nightly operator/snapshot

# debezium platform-conductor
docker buildx build --push --platform "${DEFAULT_PLATFORMS}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/platform-conductor:nightly platform-conductor/snapshot

# debezium platform-stage
docker buildx build --push --platform "${DEFAULT_PLATFORMS}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/platform-stage:nightly platform-stage/snapshot
