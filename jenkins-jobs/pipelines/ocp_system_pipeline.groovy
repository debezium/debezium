pipeline {
    agent {
        label 'Slave'
    }

    environment {
        DEBEZIUM_LOCATION = "${WORKSPACE}/debezium"
        OCP_PROJECT_NAME = "debezium-ocp-${BUILD_NUMBER}"
        OCP_PROJECT_DEBEZIUM_TESTSUITE = "${OCP_PROJECT_NAME}-testsuite"
    }

    stages {
        stage('Clean up ws') {
            steps {
                cleanWs()
            }
        }

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

        stage('Configure namespaces') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        file(credentialsId: "${params.PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    sh '''
                    oc login -u "${OCP_USERNAME}" -p "${OCP_PASSWORD}" --insecure-skip-tls-verify=true "${OCP_URL}"

                    # create testsuite project and secret
                    oc new-project "${OCP_PROJECT_DEBEZIUM_TESTSUITE}"
                    oc adm policy add-cluster-role-to-user cluster-admin "system:serviceaccount:${OCP_PROJECT_DEBEZIUM_TESTSUITE}:default"
                    oc apply -f "${SECRET_PATH}"
                    '''
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
                    env.OCP_ENV_FILE = "${WORKSPACE}/debezium-${BUILD_NUMBER}.ocp.env"
                }
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                ]) {
                    sh '''
                    set -x
                    cd "${DEBEZIUM_LOCATION}"
                    oc login ${OCP_URL} -u "${OCP_USERNAME}" --password="${OCP_PASSWORD}" --insecure-skip-tls-verify=true >/dev/null
                    ./jenkins-jobs/scripts/ocp-projects.sh --create --project "${OCP_PROJECT_NAME}" --envfile "${OCP_ENV_FILE}"
                    source "${OCP_ENV_FILE}"
                        
                    sed -i "s/namespace: .*/namespace: ${OCP_PROJECT_NAME}/" ${WORKSPACE}/strimzi/install/cluster-operator/*RoleBinding*.yaml
                    oc delete -f ${STRZ_RESOURCES} -n ${OCP_PROJECT_NAME} --ignore-not-found
                    oc create -f ${STRZ_RESOURCES} -n ${OCP_PROJECT_NAME}
                    '''
                }
            }
        }

        stage('Run testsuite') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        file(credentialsId: "${params.PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    script{
                        env.PREPARE_STRIMZI = params.STRIMZI_PREPARE_BUILD_NUMBER ? "false" : "true"
                        env.TEST_TAG_EXPRESSION = params.TEST_TAGS
                        if (!params.TEST_APICURIO_REGISTRY) {
                            env.TEST_TAG_EXPRESSION = [env.TEST_TAG_EXPRESSION, "!avro"].findAll().join(" & ")
                        }
                        env.TEST_TAG_EXPRESSION = [env.TEST_TAG_EXPRESSION, "!docker"].findAll().join(" & ")
                    }
                    sh '''
                    cd ${DEBEZIUM_LOCATION}

                    POD_DESCRIPTION="testsuite.yml"
                    PULL_SECRET_NAME=$(cat ${SECRET_PATH} | grep name | awk '{print $2;}')

                    LOG_LOCATION_IN_POD=/root/testsuite/testsuite_log

                    jenkins-jobs/docker/debezium-testing-system/deployment-template.sh --filename "${POD_DESCRIPTION}" \
                        --pull-secret-name "${PULL_SECRET_NAME}" \
                        --docker-tag "${DOCKER_TAG}" \
                        --project-name "${OCP_PROJECT_NAME}" \
                        --product-build "${PRODUCT_BUILD}" \
                        --strimzi-kc-build ${STRIMZI_KC_BUILD} \
                        --apicurio-version "${APICURIO_VERSION}" \
                        --kafka-version "${KAFKA_VERSION}" \
                        --groups-arg "${TEST_TAG_EXPRESSION}" \
                        --dbz-connect-image "${DBZ_CONNECT_IMAGE}" \
                        --artifact-server-image "${ARTIFACT_SERVER_IMAGE}" \
                        --dbz-git-repository "${DBZ_GIT_REPOSITORY}" \
                        --dbz-git-branch "${DBZ_GIT_BRANCH}" \
                        --testsuite-log "${LOG_LOCATION_IN_POD}" \
                        --strimzi-channel "${STRZ_CHANNEL}" \
                        --apicurio-channel "${APIC_CHANNEL}" \
                        --prepare-strimzi "${PREPARE_STRIMZI}"

                    oc project "${OCP_PROJECT_DEBEZIUM_TESTSUITE}"
                    oc create -f "${POD_DESCRIPTION}"
                    pod_name=$(oc get pods | tail -1 | awk '{print $1;}')
                    
                    {
                        oc wait --timeout=10h --for=condition=Ready pod/${pod_name}
                        
                        # copy log and test results                 
                        mkdir ${WORKSPACE}/testsuite_artifacts
                        oc rsync ${pod_name}:${LOG_LOCATION_IN_POD} ${WORKSPACE}/testsuite_artifacts
                        oc rsync ${pod_name}:/root/testsuite/artifacts.zip ${WORKSPACE}/testsuite_artifacts || oc delete pod ${pod_name}
                        oc delete pod ${pod_name}
                    } & 
                    
                    # wait for container to start and print logs
                    for i in {1..100}; do
                        sleep 2
                        oc logs -f ${pod_name}  && break
                    done
                    '''
                }
            }
        }
    }
    post {
        always {
            sh '''
            cd ${DEBEZIUM_LOCATION}
            ./jenkins-jobs/scripts/ocp-projects.sh --delete --testsuite --project ${OCP_PROJECT_NAME}
            '''
            archiveArtifacts "**/testsuite_artifacts/*"
            withCredentials([
                    usernamePassword(credentialsId: "rh-integration-quay-creds", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                    string(credentialsId: "report-portal-token", variable: 'RP_TOKEN'),
            ]) {
                sh '''
                if [ "${PRODUCT_BUILD}" == true ] ; then
                    export ATTRIBUTES="downstream ocp"
                else
                    export ATTRIBUTES="upstream ocp"
                fi

                cd ${WORKSPACE}/testsuite_artifacts
                mkdir results    
                unzip artifacts.zip -d results
                
                RESULTS_FOLDER="."
                rm -rf ${RESULTS_FOLDER}/failsafe-summary.xml            
                
                docker login quay.io -u "$QUAY_USERNAME" -p "$QUAY_PASSWORD"

                ${DEBEZIUM_LOCATION}/jenkins-jobs/scripts/report.sh --connector false --env-file env-file.env --results-folder ${RESULTS_FOLDER} --attributes "${ATTRIBUTES}"
                '''
            }
        }
    }
}
