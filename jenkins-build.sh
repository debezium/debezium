#!/bin/bash

set -eux
. /usr/stripe/bin/docker/stripe-init-build

release_version=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)
git_sha=$(git rev-parse --short HEAD)

/usr/local/bin/junit-script-output "running checkstyle" ./checkstyle.sh

echo "Building Debezium version $release_version."
mvn clean install -Passembly -DskipITs -pl debezium-bom,debezium-core,debezium-connector-mongodb

if [[ "${GIT_BRANCH:-}" == "master" ]] || [[ "${GIT_BRANCH:-}" == *_FORCE_DEPLOY ]]; then
    echo "Publishing Debezium version $release_version to Artifactory."
    mvn deploy:deploy-file \
        -DgroupId=io.debezium -DrepositoryId=stripe-artifactory-v2 -Durl=https://artifactory-content.stripe.build/artifactory/maven-snapshots-local/ \
        -DartifactId=debezium-connector-mongodb -Dversion=stripe-$git_sha-$release_version\
        -Dpackaging=tar.gz -Dfile=debezium-connector-mongodb/target/debezium-connector-mongodb-$release_version-plugin.tar.gz
else
    echo "Skip publishing to Artifactory."
fi

