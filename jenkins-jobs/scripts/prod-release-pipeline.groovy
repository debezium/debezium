if (
    !BUILD_VERSION ||
    !PRODUCT_VERSION ||
    !USERNAME ||
    !PASSWORD ||
    !CONNECTORS ||
    !SOURCE_MAVEN_REPO ||
    !TARGET_HOST ||
    !ARTIFACT_DIR
) {
    error 'Input parameters not provided'
}

SOURCES_DIR='src'
TARGET_DIR="${ARTIFACT_DIR}/${PRODUCT_VERSION}"
REMOTE_TARGET = [
    'name': 'stage',
    'host': TARGET_HOST,
    'user': USERNAME,
    'password': PASSWORD,
    'allowAnyHosts': true
]

node('Slave') {
    stage ('Download package artifacts from repo') {
        withCredentials([string(credentialsId: SOURCE_MAVEN_REPO, variable: 'SOURCE_MAVEN_REPO')]) {
            sh """
                rm -rf *
                for CONNECTOR in \${CONNECTORS}; do
                    curl -OLs "\${SOURCE_MAVEN_REPO}/debezium-connector-\$CONNECTOR/${BUILD_VERSION}/debezium-connector-\$CONNECTOR-${BUILD_VERSION}-plugin.zip"
                done
            """
        }
    }
    stage ('Download and repackage sources') {
        withCredentials([string(credentialsId: SOURCE_MAVEN_REPO, variable: 'SOURCE_MAVEN_REPO')]) {
            sh """
                mkdir "${SOURCES_DIR}"
                curl -Lv "\${SOURCE_MAVEN_REPO}/debezium-parent/${BUILD_VERSION}/debezium-parent-${BUILD_VERSION}-project-sources.tar.gz" | tar xz --strip-components=1 -C "${SOURCES_DIR}"
                (cd "${SOURCES_DIR}" && zip -r "../debezium-${BUILD_VERSION}-src.zip" *)
                rm -rf "${SOURCES_DIR}"
                ls -al
            """
        }
    }
    stage ('Upload artifacts') {
        withCredentials([string(credentialsId: TARGET_HOST, variable: 'TARGET_HOST')]) {
            sh """
                set +x
                docker run --rm -v \$(pwd):/upload ictu/sshpass -p ${PASSWORD} rsync -va -e \"ssh -o StrictHostKeyChecking=no\" --include='*.zip' /upload/ ${USERNAME}@\${TARGET_HOST}:${TARGET_DIR}
                docker run --rm ictu/sshpass -p ${PASSWORD} ssh -o StrictHostKeyChecking=no ${USERNAME}@\${TARGET_HOST} ls -al ${TARGET_DIR}
            """
            if (STAGE_FILES) {
                sh """
                    set +x
                    docker run --rm ictu/sshpass -p ${PASSWORD} ssh -o StrictHostKeyChecking=no ${USERNAME}@\${TARGET_HOST} /mnt/redhat/scripts/rel-eng/utility/bus-clients/stage-mw-release ${PRODUCT_VERSION}
                """
            }
        }
    }
}
