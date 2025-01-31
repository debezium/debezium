#!/bin/bash

DEBEZIUM_REPOSITORY=debezium/debezium
DEBEZIUM_BRANCH=main
DEFAULT_PLATFORMS=linux/amd64,linux/arm64

SNAPSHOT_VERSION=$(curl -s https://raw.githubusercontent.com/$DEBEZIUM_REPOSITORY/$DEBEZIUM_BRANCH/pom.xml | grep -o '<version>.*-SNAPSHOT</version>' | awk -F '[<>]' '{print $3}')

docker login -u ${QUAYIO_CREDENTIALS%:*} -p ${QUAYIO_CREDENTIALS#*:} quay.io

# connect
docker buildx build  --platform "${PLATFORM}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/connect:nightly connect/snapshot
docker push quay.io/debezium/connect:nightly

# server
docker buildx build  --platform "${PLATFORM}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/server:nightly server/snapshot
docker push quay.io/debezium/server:nightly

# operator
docker buildx build  --platform "${PLATFORM}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/operator:nightly operator/snapshot
docker push quay.io/debezium/operator:nightly

# debezium platform-conductor
docker buildx build --platform "${PLATFORM}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/platform-conductor:nightly platform-conductor/snapshot
docker push quay.io/debezium/platform-conductor:nightly

# debezium platform-stage
docker buildx build --platform "${PLATFORM}" --build-arg DEBEZIUM_VERSION=$SNAPSHOT_VERSION -t quay.io/debezium/platform-stage:nightly platform-stage/snapshot
docker push quay.io/debezium/platform-stage:nightly
