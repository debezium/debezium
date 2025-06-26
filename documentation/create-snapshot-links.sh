#!/bin/bash
# Maven Central does not provide a permanent link to latest snapshot
# The script thus obtain the latest snapshot version and updates the links in documentation to point to it

MAVEN_REPO="https://central.sonatype.com/repository/maven-snapshots"
GROUP_ID="io/debezium"
DEBEZIUM_VERSION="3.2.0-SNAPSHOT"
ARTIFACT_PREFIX="debezium-connector"
FILE_EXT="tar.gz"
ANTORA_FILE="antora.yml"
CONNECTORS=(mysql mongodb postgres sqlserver oracle db2 jdbc spanner vitess informix ibmi mariadb cassandra-3 cassandra-4 cassandra-5)

update_snapshot_link() {
    local COMPONENT=$1
    local ARTIFACT_ID=$2
    local CLASSIFIER=$3

    SNAPSHOT_VERSION=$(curl --silent -fSL $MAVEN_REPO/$GROUP_ID/$ARTIFACT_ID/$DEBEZIUM_VERSION/maven-metadata.xml | awk -F'<[^>]+>' '/<extension>tar.gz<\/extension>/ {getline; print $2; exit}')
    SNAPSHOT_LINK="$MAVEN_REPO/$GROUP_ID/$ARTIFACT_ID/$DEBEZIUM_VERSION/$ARTIFACT_ID-${SNAPSHOT_VERSION}${CLASSIFIER}.tar.gz"
    sed -i "s#link-$COMPONENT-snapshot:.*#link-$COMPONENT-snapshot: \'$SNAPSHOT_LINK\'#" $ANTORA_FILE
}

for CONNECTOR in "${CONNECTORS[@]}"; do
    update_snapshot_link "$CONNECTOR-plugin" "debezium-connector-$CONNECTOR" "-plugin"
done

update_snapshot_link "server" "debezium-server-dist" ""
