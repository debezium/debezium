pipeline {
    agent {
        label 'Slave'
    }

    environment {
        DEBEZIUM_LOCATION = "${WORKSPACE}/debezium"
        OCP_PROJECT_NAME = "debezium-${BUILD_NUMBER}"
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

        stage('Copy artifacts') {
            when {
                expression { params.PRODUCT_BUILD }
            }
            stages {
                stage('Copy apicurio artifacts - latest') {
                    when {
                        expression { !params.APICURIO_PREPARE_BUILD_NUMBER }
                    }
                    steps {
                        copyArtifacts projectName: 'ocp-downstream-apicurio-prepare-job',
                                target: "${WORKSPACE}/apicurio",
                                filter: 'apicurio-registry-install-examples.zip',
                                selector: lastSuccessful()
                    }
                }
                stage('Copy apicurio artifacts') {
                    when {
                        expression { params.APICURIO_PREPARE_BUILD_NUMBER }
                    }
                    steps {
                        copyArtifacts projectName: 'ocp-downstream-apicurio-prepare-job',
                                target: "${WORKSPACE}/apicurio",
                                filter: 'apicurio-registry-install-examples.zip',
                                selector: specific(params.APICURIO_PREPARE_BUILD_NUMBER)
                    }
                }

                stage('Copy strimzi artifacts - latest') {
                    when {
                        expression { !params.STRIMZI_PREPARE_BUILD_NUMBER }
                    }
                    steps {
                        copyArtifacts projectName: 'ocp-downstream-strimzi-prepare-job',
                                target: "${WORKSPACE}/strimzi",
                                filter: 'amq-streams-install-examples.zip',
                                selector: lastSuccessful()
                    }
                }
                stage('Copy strimzi artifacts') {
                    when {
                        expression { params.STRIMZI_PREPARE_BUILD_NUMBER }
                    }
                    steps {
                        copyArtifacts projectName: 'ocp-downstream-strimzi-prepare-job',
                                target: "${WORKSPACE}/strimzi",
                                filter: 'amq-streams-install-examples.zip',
                                selector: specific(params.STRIMZI_PREPARE_BUILD_NUMBER)
                    }
                }
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

                    # create projects
                    cd ${DEBEZIUM_LOCATION}
                    ./jenkins-jobs/scripts/ocp-projects.sh --project ${OCP_PROJECT_NAME} --create --testsuite
                    source ./${OCP_PROJECT_NAME}.ocp.env

                    # create secret
                    oc project "${OCP_PROJECT_DEBEZIUM_TESTSUITE}"
                    oc adm policy add-cluster-role-to-user cluster-admin "system:serviceaccount:${OCP_PROJECT_DEBEZIUM_TESTSUITE}:default"
                    oc apply -f "${SECRET_PATH}"
                    oc apply -f "${SECRET_PATH}" -n "${OCP_PROJECT_REGISTRY}"
                    '''
                }
            }
        }

        stage('Prepare strimzi - upstream') {
            when {
                expression { !params.PRODUCT_BUILD }
            }
            steps {
                sh '''
                source ${DEBEZIUM_LOCATION}/${OCP_PROJECT_NAME}.ocp.env
                mkdir ${WORKSPACE}/strimzi && cd ${WORKSPACE}/strimzi
                git clone --branch "${STRZ_GIT_BRANCH}" "${STRZ_GIT_REPOSITORY}" . || exit 2 ;
                sed -i 's/namespace: .*/namespace: '"${OCP_PROJECT_DEBEZIUM}"'/' install/cluster-operator/*RoleBinding*.yaml ;
                oc apply -f install/cluster-operator/ -n "${OCP_PROJECT_DEBEZIUM}"  || true ;
                '''
            }
        }

        stage('Prepare strimzi - downstream') {
            when {
                expression { params.PRODUCT_BUILD }
            }
            steps {
                sh '''
                source ${DEBEZIUM_LOCATION}/${OCP_PROJECT_NAME}.ocp.env
                cd ${WORKSPACE}/strimzi
                unzip ./*.zip;
                sed -i 's/namespace: .*/namespace: '"${OCP_PROJECT_DEBEZIUM}"'/' install/cluster-operator/*RoleBinding*.yaml ;
                oc apply -f install/cluster-operator/ -n "${OCP_PROJECT_DEBEZIUM}"  || true ;
                '''
            }
        }

        stage('Prepare apicurio - upstream') {
            when {
                expression { !params.PRODUCT_BUILD && params.TEST_APICURIO_REGISTRY }
            }
            steps {
                withCredentials([
                    file(credentialsId: "${params.PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    sh '''
                    source ${DEBEZIUM_LOCATION}/${OCP_PROJECT_NAME}.ocp.env
                    mkdir ${WORKSPACE}/apicurio && cd ${WORKSPACE}/apicurio
                    git clone --branch "${APIC_GIT_BRANCH}" "${APIC_GIT_REPOSITORY}" . || exit 2 ;

                    APICURIO_RESOURCE="install/apicurio-registry-operator-*-dev.yaml"

                    sed -i "s/namespace: apicurio-registry-operator-namespace/namespace: ${OCP_PROJECT_REGISTRY}/" install/*.yaml ;
                    oc apply -f ${APICURIO_RESOURCE} -n "${OCP_PROJECT_REGISTRY}" || true ;
                    '''
                }
            }
        }

        stage('Prepare apicurio - downstream') {
            when {
                expression { params.PRODUCT_BUILD && params.TEST_APICURIO_REGISTRY }
            }
            steps {
                withCredentials([
                    file(credentialsId: "${params.PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    sh '''
                    source ${DEBEZIUM_LOCATION}/${OCP_PROJECT_NAME}.ocp.env
                    cd ${WORKSPACE}/apicurio
                    unzip ./*.zip;

                    APICURIO_RESOURCE="install/install.yaml"

                    sed -i "s/namespace: apicurio-registry-operator-namespace/namespace: ${OCP_PROJECT_REGISTRY}/" install/*.yaml ;
                    oc apply -f ${APICURIO_RESOURCE} -n "${OCP_PROJECT_REGISTRY}" || true ;
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
                    sh '''
                    cd ${DEBEZIUM_LOCATION}
                    source ./${OCP_PROJECT_NAME}.ocp.env

                    DBZ_GROUPS_ARG="!docker"
                    if [ ${TEST_APICURIO_REGISTRY} == false ]; then
                        DBZ_GROUPS_ARG="${DBZ_GROUPS_ARG} & !avro"
                    fi

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
                        --groups-arg "${DBZ_GROUPS_ARG}" \
                        --dbz-connect-image "${DBZ_CONNECT_IMAGE}" \
                        --artifact-server-image "${ARTIFACT_SERVER_IMAGE}" \
                        --dbz-git-repository "${DBZ_GIT_REPOSITORY}" \
                        --dbz-git-branch "${DBZ_GIT_BRANCH}" \
                        --testsuite-log "${LOG_LOCATION_IN_POD}"

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
