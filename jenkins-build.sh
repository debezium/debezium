#!/bin/bash

set -eux
. /usr/stripe/bin/docker/stripe-init-build

release_version=stripe-$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)

echo "Building Debezium version $release_version."
mvn clean install -Passembly -DskipITs -DskipTests -pl debezium-connector-mongodb

# stripe-sync-dir "debezium-connector-mongodb/target/debezium-connector-mongodb-$release_version-plugin.tar.gz" /build
test debezium-connector-mongodb/target/debezium-connector-mongodb-$release_version-plugin.tar.gz

echo "Publishing Debezium version $release_version to Artifactory."
# if [[ "${GIT_BRANCH:-}" == "master" ]] || [[ "${GIT_BRANCH:-}" == *_FORCE_DEPLOY ]]; then
# fi
mvn deploy:deploy-file \
    -DgroupId=io.debezium -DrepositoryId=stripe-artifactory-v2 -Durl=https://artifactory-content.stripe.build/artifactory \
    -Dpackaging=tar.gz -Dfile=debezium-connector-mongodb/target/debezium-connector-mongodb-$release_version-plugin.tar.gz
