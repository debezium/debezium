pipeline {
    agent {
        label 'NodeXL'
    }

    stages {
        stage('Checkout - Debezium') {
            steps {
                checkout([
                        $class           : 'GitSCM',
                        branches         : [[name: "${DBZ_GIT_BRANCH}"]],
                        userRemoteConfigs: [[url: "${DBZ_GIT_REPOSITORY}"]],
                        extensions       : [[$class           : 'RelativeTargetDirectory',
                                             relativeTargetDir: 'debezium']],
                ])
            }
        }

        stage('Checkout - Debezium DB2 connector') {
            when {
                expression { !params.PRODUCT_BUILD }
            }
            steps {
                checkout([
                        $class           : 'GitSCM',
                        branches         : [[name: "${DBZ_GIT_BRANCH_DB2}"]],
                        userRemoteConfigs: [[url: "${DBZ_GIT_REPOSITORY_DB2}"]],
                        extensions       : [[$class           : 'RelativeTargetDirectory',
                                             relativeTargetDir: 'debezium-connector-db2']],
                ])
            }
        }

        stage('Configure') {
            steps {
                script {
                    if (!params.PRODUCT_BUILD && params.STRIMZI_PREPARE_BUILD_NUMBER) {
                        error("Using productised strimzi archive in upstream build")
                    }

                    env.OCP_ENV_FILE = "${WORKSPACE}/debezium-${BUILD_NUMBER}.ocp.env"
                    env.MVN_PROFILE_PROD = params.PRODUCT_BUILD ? "-Pproduct" : ""
                    env.PREPARE_STRIMZI_OPERATOR = params.STRIMZI_PREPARE_BUILD_NUMBER ? "-Dprepare.strimzi=false" : ""
                    env.OCP_PROJECT_DEBEZIUM = "debezium-${currentBuild.number}"

//                    Use strimzi build mechanism unless pre-built KC image is provided
                    env.TEST_CONNECT_STRZ_BUILD = params.IMAGE_CONNECT_STRZ ? false : true

//                    Configure images if provided
                    env.MVN_IMAGE_CONNECT_STRZ = params.IMAGE_CONNECT_STRZ ? "-Dimage.kc=${params.IMAGE_CONNECT_STRZ}" : ""
                    env.MVN_IMAGE_CONNECT_RHEL = params.IMAGE_CONNECT_RHEL ? "-Ddocker.image.kc=${params.IMAGE_CONNECT_RHEL}" : ""
                    env.MVN_IMAGE_DBZ_AS = params.IMAGE_DBZ_AS ? "-Dimage.as=${params.IMAGE_DBZ_AS}" : ""

//                    Test tag configuration
                    env.TEST_TAG_EXPRESSION = params.TEST_TAGS
                    if (!params.TEST_APICURIO_REGISTRY) {
                        env.TEST_TAG_EXPRESSION = [env.TEST_TAG_EXPRESSION, "!avro"].findAll().join(" & ")
                    }

//                    Version configuration
                    env.MVN_VERSION_KAFKA = params.TEST_VERSION_KAFKA ? "-Dversion.kafka=${params.TEST_VERSION_KAFKA}" : ""
                    env.MVN_VERSION_AS_DEBEZIUM = params.AS_VERSION_DEBEZIUM ? "-Das.debezium.version=${params.AS_VERSION_DEBEZIUM}" : ""
                    env.MVN_VERSION_AS_APICURIO = params.AS_VERSION_APICURIO ? "-Das.apicurio.version=${params.AS_VERSION_APICURIO}" : ""

                }
            }
        }

        stage('Checkout Downstream AMQ Streams') {
            when {
                expression { params.PRODUCT_BUILD && params.STRIMZI_PREPARE_BUILD_NUMBER }
            }
            steps {
                copyArtifacts projectName: 'ocp-downstream-strimzi-prepare-job',
                        filter: 'amq-streams-install-examples.zip',
                        selector: specific(params.STRIMZI_PREPARE_BUILD_NUMBER)
                unzip zipFile: 'amq-streams-install-examples.zip', dir: 'strimzi'
            }
        }

        stage('Prepare Downstream AMQ Streams') {
            when {
                expression { params.PRODUCT_BUILD && params.STRIMZI_PREPARE_BUILD_NUMBER }
            }
            steps {
                script {
                    env.STRZ_RESOURCES = "${env.WORKSPACE}/strimzi/install/cluster-operator"
                }
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),

                ]) {
                    sh '''
                    set -x
                    cd "${WORKSPACE}/debezium"
                    oc login ${OCP_URL} -u "${OCP_USERNAME}" --password="${OCP_PASSWORD}" --insecure-skip-tls-verify=true >/dev/null
                    ./jenkins-jobs/scripts/ocp-projects.sh --create -t "${BUILD_NUMBER}" --envfile "${OCP_ENV_FILE}"
                    source "${OCP_ENV_FILE}"
                        
                    sed -i "s/namespace: .*/namespace: ${OCP_PROJECT_DEBEZIUM}/" ${WORKSPACE}/strimzi/install/cluster-operator/*RoleBinding*.yaml
                    oc delete -f ${STRZ_RESOURCES} -n ${OCP_PROJECT_DEBEZIUM} --ignore-not-found
                    oc create -f ${STRZ_RESOURCES} -n ${OCP_PROJECT_DEBEZIUM}
                    '''
                }
            }
        }

        stage('Build') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),

                ]) {
                    sh '''
                    set -x
                    docker login -u=${QUAY_USERNAME} -p=${QUAY_PASSWORD} quay.io
                    oc login ${OCP_URL} -u "${OCP_USERNAME}" --password="${OCP_PASSWORD}" --insecure-skip-tls-verify=true >/dev/null
                    '''

                    sh '''
                    set -x
                    cd ${WORKSPACE}/debezium
                    ORACLE_ARTIFACT_VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=version.oracle.driver)
                    ORACLE_ARTIFACT_DIR="${HOME}/oracle-libs/${ORACLE_ARTIFACT_VERSION}.0"

                    cd ${ORACLE_ARTIFACT_DIR}
                    mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=ojdbc8.jar
                    mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=xstreams.jar
                    '''
                }

                sh '''
                set -x
                cd "${WORKSPACE}/debezium"
                mvn clean install -DskipTests -DskipITs
                '''
            }
        }

        stage('Build -- Upstream') {
            when {
                expression { !params.PRODUCT_BUILD }
            }
            steps {
//              Build DB2 Connector
                sh '''
                set -x
                cd ${WORKSPACE}/debezium-connector-db2
                mvn clean install -DskipTests -DskipITs -Passembly
                '''
//              Build Oracle connector
                sh '''
                set -x
                cd ${WORKSPACE}/debezium
                mvn install -Passembly,oracle-all -DskipTests -DskipITs
                '''
            }
        }

        stage('Enable debug') {
            when {
                expression { params.DEBUG_MODE }
            }
            steps {
                script {
                    env.MAVEN_OPTS = "-DforkCount=0 -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=*:5005"
                }
            }
        }

        stage('Test') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        file(credentialsId: "${params.PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    sh '''
                    set -x
                    cd ${WORKSPACE}/debezium

                    mvn install -pl debezium-testing/debezium-testing-system -PsystemITs,oracleITs \\
                    ${MVN_PROFILE_PROD} \\
                    ${PREPARE_STRIMZI_OPERATOR} \\
                    -Docp.project.debezium="${OCP_PROJECT_DEBEZIUM}" \\
                    -Docp.username="${OCP_USERNAME}" \\
                    -Docp.password="${OCP_PASSWORD}" \\
                    -Docp.url="${OCP_URL}" \\
                    -Docp.pull.secret.paths="${SECRET_PATH}" \\
                    -Dstrimzi.kc.build=${TEST_CONNECT_STRZ_BUILD} \\
                    -Dtest.wait.scale="${TEST_WAIT_SCALE}" \\
                    ${MVN_IMAGE_CONNECT_STRZ} \\
                    ${MVN_IMAGE_CONNECT_RHEL} \\
                    ${MVN_IMAGE_DBZ_AS} \\
                    ${MVN_VERSION_KAFKA} \\
                    ${MVN_VERSION_AS_DEBEZIUM} \\
                    ${MVN_VERSION_AS_APICURIO} \\
                    -Dstrimzi.operator.channel=${STRIMZI_OPERATOR_CHANNEL} \\
                    -Dapicurio.operator.channel=${APICURIO_OPERATOR_CHANNEL} \\
                    -Dgroups="${TEST_TAG_EXPRESSION}"
                    '''
                }
            }
        }
    }

    post {
        always {
            archiveArtifacts '**/target/failsafe-reports/*.xml'
            junit '**/target/failsafe-reports/*.xml'

            mail to: params.MAIL_TO, subject: "Debezium OpenShift test run #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: """
OpenShift interoperability test run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
            withCredentials([
                    usernamePassword(credentialsId: "rh-integration-quay-creds", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                    string(credentialsId: "report-portal-token", variable: 'RP_TOKEN'),
            ]) {
                sh '''
                if [ "$PRODUCT_BUILD" == true ] ; then
                    export ATTRIBUTES="downstream"
                else
                    export ATTRIBUTES="upstream"
                fi

                RESULTS_FOLDER=final-results
                RESULTS_PATH=$RESULTS_FOLDER/results
                
                mkdir -p $RESULTS_PATH
                cp debezium/debezium-testing/debezium-testing-system/target/failsafe-reports/*.xml $RESULTS_PATH
                rm -rf $RESULTS_PATH/failsafe-summary.xml
                
                docker login quay.io -u "$QUAY_USERNAME" -p "$QUAY_PASSWORD"

                ./debezium/jenkins-jobs/scripts/report.sh --connector false --env-file env-file.env --results-folder $RESULTS_FOLDER --attributes $ATTRIBUTES
                '''
            }
        }
    }
}
