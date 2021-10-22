if (
    !PRODUCT_VERSION ||
    !PRODUCT_VERSION_RELEASE ||
    !PRODUCT_MILESTONE ||
    !DEBEZIUM_VERSION ||
    !BACON_REPO ||
    !BACON_VERSION ||
    !BACON_CONFIG_URL ||
    !USERNAME ||
    !PASSWORD ||
    !TARGET_HOST ||
    !ARTIFACT_DIR
) {
    error 'Input parameters not provided'
}

CONFIG_DIR = '.config/pnc-bacon'
BUILD_CONFIG_DIR = 'debezium-bacon'
BACON_MAVEN_URL = "https://repo1.maven.org/maven2/org/jboss/pnc/bacon/cli/$BACON_VERSION/cli-$BACON_VERSION-shaded.jar"
CERTIFICATES = ['Eng_Ops_CA.crt', 'Red_Hat_IS_CA.crt',  'RH-IT-Root-CA.crt']
BUILD_VERSION = "${PRODUCT_VERSION}.${PRODUCT_VERSION_RELEASE}.${PRODUCT_MILESTONE}"
RELEASE_VERSION = "AMQ-CDC-${PRODUCT_VERSION}.${PRODUCT_MILESTONE}"
PACKAGES_DIR = "target/debezium-integration-${BUILD_VERSION}"
TARGET_DIR = "${ARTIFACT_DIR}/${RELEASE_VERSION}"
REMOTE_TARGET = [
    'name': 'stage',
    'host': TARGET_HOST,
    'user': USERNAME,
    'password': PASSWORD,
    'allowAnyHosts': true
]

node('Slave') {
    try {
        stage ('Download and setup bacon') {
            sh """
                rm -rf *
                curl -k -o bacon.jar $BACON_MAVEN_URL
                mkdir -p $BUILD_CONFIG_DIR $HOME/$CONFIG_DIR
                curl -k -o $BUILD_CONFIG_DIR/build-config.yaml '$BACON_CONFIG_URL;f=build-config.yaml'
                curl -k -o $HOME/$CONFIG_DIR/config.yaml '$BACON_CONFIG_URL;f=config.yaml'
            """
            for (certificate in CERTIFICATES) {
                sh (script: """
                    curl -k -o $certificate '$BACON_CONFIG_URL;f=$certificate'
                    keytool -importcert -cacerts -alias $certificate -file $certificate -noprompt -storepass changeit
                """, returnStatus: true)
            }
        }
        stage ('Build product') {
            sh "java -jar ./bacon.jar pig run ${TEMPORARY_BUILD == 'true' ? '-t' : ''} -e product-version=$PRODUCT_VERSION -e product-version-release=$PRODUCT_VERSION_RELEASE -e debezium-version=$DEBEZIUM_VERSION -e milestone=$PRODUCT_MILESTONE -v debezium-bacon"
        }
        stage('Extract connector packages') {
            dir (PACKAGES_DIR) {
                sh "unzip -j debezium-*-maven-repository.zip '*.zip'"
            }
        }

        stage('Upload artifacts') {
            dir (PACKAGES_DIR) {
                withCredentials([string(credentialsId: TARGET_HOST, variable: 'TARGET_HOST')]) {
                    sh """
                        set +x
                        docker run --rm -v \$(pwd):/upload ictu/sshpass -p ${PASSWORD} rsync -va -e \"ssh -o StrictHostKeyChecking=no\" --include='*.zip' /upload/ ${
                        USERNAME
                    }@\${TARGET_HOST}:${TARGET_DIR}
                        docker run --rm ictu/sshpass -p ${PASSWORD} ssh -o StrictHostKeyChecking=no ${USERNAME}@\${TARGET_HOST} ls -al ${TARGET_DIR}
                    """
                    if (STAGE_FILES) {
                        sh """
                            set +x
                            docker run --rm ictu/sshpass -p ${PASSWORD} ssh -o StrictHostKeyChecking=no ${
                            USERNAME
                        }@\${TARGET_HOST} /mnt/redhat/scripts/rel-eng/utility/bus-clients/stage-mw-release ${RELEASE_VERSION}
                        """
                    }
                }
            }
        }
    } finally {
        mail to: MAIL_TO, subject: "${JOB_NAME} run #${BUILD_NUMBER} finished", body: "Run ${BUILD_URL} finished with result: ${currentBuild.currentResult}"
    }
}
