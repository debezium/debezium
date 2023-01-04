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

        stage('Checkout - Upstream Strimzi') {
            when {
                expression { !params.PRODUCT_BUILD }
            }
            steps {
                checkout([
                        $class           : 'GitSCM',
                        branches         : [[name: "${STRZ_GIT_BRANCH}"]],
                        userRemoteConfigs: [[url: "${STRZ_GIT_REPOSITORY}"]],
                        extensions       : [[$class           : 'RelativeTargetDirectory',
                                             relativeTargetDir: 'strimzi']],
                ])
                script {
                    env.STRZ_RESOURCES = "${env.WORKSPACE}/strimzi/install/cluster-operator"
                }
            }
        }

        stage('Checkout - Downstream AMQ Streams') {
            when {
                expression { params.PRODUCT_BUILD }
            }
            steps {
                script {
                    env.STRZ_RESOURCES = "${env.WORKSPACE}/strimzi/install/cluster-operator"
                }
                copyArtifacts projectName: 'ocp-downstream-strimzi-prepare-job', filter: 'amq-streams-install-examples.zip', selector: lastSuccessful()
                unzip zipFile: 'amq-streams-install-examples.zip', dir: 'strimzi'
            }
        }

        stage('Checkout - Upstream Apicurio') {
            when {
                expression { !params.PRODUCT_BUILD && params.TEST_APICURIO_REGISTRY }
            }
            steps {
                error('Upstream Apicurio testing is not supported by the pipeline')
            }
        }

        stage('Checkout - Downstream Service registry') {
            when {
                expression { params.PRODUCT_BUILD && params.TEST_APICURIO_REGISTRY }
            }
            steps {
                script {
                    env.APIC_RESOURCES = "${env.WORKSPACE}/apicurio/install/"
                }
                copyArtifacts projectName: 'ocp-downstream-apicurio-prepare-job', filter: 'apicurio-registry-install-examples.zip', selector: lastSuccessful()
                unzip zipFile: 'apicurio-registry-install-examples.zip', dir: 'apicurio'
            }
        }

        stage('Configure') {
            steps {
                script {
                    env.OCP_ENV_FILE = "${WORKSPACE}/debezium-${BUILD_NUMBER}.ocp.env"
                    env.MVN_PROFILE_PROD = params.PRODUCT_BUILD ? "-Pproduct" : ""

//                    Use strimzi build mechanism unless pre-built KC image is provided
                    env.TEST_CONNECT_STRZ_BUILD = params.IMAGE_CONNECT_STRZ ? false : true

//                    Configure images if provided
                    env.IMAGE_TAG_SUFFIX = "${BUILD_NUMBER}"
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
                    cd "${WORKSPACE}/debezium"
                    ./jenkins-jobs/scripts/ocp-projects.sh --create -t "${BUILD_NUMBER}" --envfile "${OCP_ENV_FILE}"
                    source "${OCP_ENV_FILE}"
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

                    sh '''
                    set -x
                    source "${OCP_ENV_FILE}"
                    sed -i "s/namespace: .*/namespace: ${OCP_PROJECT_DEBEZIUM}/" strimzi/install/cluster-operator/*RoleBinding*.yaml
                    oc delete -f ${STRZ_RESOURCES} -n ${OCP_PROJECT_DEBEZIUM} --ignore-not-found
                    oc create -f ${STRZ_RESOURCES} -n ${OCP_PROJECT_DEBEZIUM}
                    '''
                }
            }
        }

        stage('Configure - Apicurio') {
            when {
                expression { params.TEST_APICURIO_REGISTRY }
            }
            steps {
                script {
                    env.OCP_PROJECT_REGISTRY = "debezium-${BUILD_NUMBER}-registry"
                }
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),

                ]) {
                    sh '''
                    set -x
                    oc login ${OCP_URL} -u "${OCP_USERNAME}" --password="${OCP_PASSWORD}" --insecure-skip-tls-verify=true >/dev/null
                    '''
                    sh '''
                    set -x
                    source "${OCP_ENV_FILE}"
                    cat ${APIC_RESOURCES}/install.yaml | grep "namespace: apicurio-registry-operator-namespace" -A5 -B5
                    sed -i "s/namespace: apicurio-registry-operator-namespace/namespace: ${OCP_PROJECT_REGISTRY}/" ${APIC_RESOURCES}/install.yaml
                    cat ${APIC_RESOURCES}/install.yaml | grep "namespace: ${OCP_PROJECT_REGISTRY}" -A5 -B5
                    oc delete -f ${APIC_RESOURCES} -n ${OCP_PROJECT_REGISTRY} --ignore-not-found
                    oc create -f ${APIC_RESOURCES} -n ${OCP_PROJECT_REGISTRY}
                    '''
                }
            }
        }

        stage('Build') {
            steps {
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
                    source "${OCP_ENV_FILE}"

                    mvn install -pl debezium-testing/debezium-testing-system -PsystemITs,oracleITs \\
                    ${MVN_PROFILE_PROD} \\
                    -Docp.project.debezium="${OCP_PROJECT_DEBEZIUM}" \\
                    -Docp.project.registry="${OCP_PROJECT_REGISTRY}" \\
                    -Docp.project.mysql="${OCP_PROJECT_MYSQL}"  \\
                    -Docp.project.postgresql="${OCP_PROJECT_POSTGRESQL}" \\
                    -Docp.project.sqlserver="${OCP_PROJECT_SQLSERVER}"  \\
                    -Docp.project.mongo="${OCP_PROJECT_MONGO}" \\
                    -Docp.project.db2="${OCP_PROJECT_DB2}" \\
                    -Docp.project.oracle="${OCP_PROJECT_ORACLE}" \\
                    -Docp.username="${OCP_USERNAME}" \\
                    -Docp.password="${OCP_PASSWORD}" \\
                    -Docp.url="${OCP_URL}" \\
                    -Docp.pull.secret.paths="${SECRET_PATH}" \\
                    -Dstrimzi.kc.build=${TEST_CONNECT_STRZ_BUILD} \\
                    -Dimage.tag.suffix="${IMAGE_TAG_SUFFIX}" \\
                    -Dtest.wait.scale="${TEST_WAIT_SCALE}" \\
                    ${MVN_IMAGE_CONNECT_STRZ} \\
                    ${MVN_IMAGE_CONNECT_RHEL} \\
                    ${MVN_IMAGE_DBZ_AS} \\
                    ${MVN_VERSION_KAFKA} \\
                    ${MVN_VERSION_AS_DEBEZIUM} \\
                    ${MVN_VERSION_AS_APICURIO} \\
                    -Dgroups="${TEST_TAG_EXPRESSION}"
                    '''
                }
            }
        }
    }

    post {
        always {
            sh '''
            cd ${WORKSPACE}/debezium
            ./jenkins-jobs/scripts/ocp-projects.sh --delete -t ${BUILD_NUMBER}
            '''
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
