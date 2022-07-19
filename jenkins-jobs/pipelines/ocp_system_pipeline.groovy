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
                        copyArtifacts projectName: 'ocp-downstream-apicurio-prepare-job', target: "${DEBEZIUM_LOCATION}/apicurio" ,filter: 'apicurio-registry-install-examples.zip', selector: lastSuccessful()
                    }
                }
                stage('Copy apicurio artifacts') {
                    when {
                        expression { params.APICURIO_PREPARE_BUILD_NUMBER }
                    }
                    steps {
                        copyArtifacts projectName: 'ocp-downstream-apicurio-prepare-job', target: "${DEBEZIUM_LOCATION}/apicurio" , filter: 'apicurio-registry-install-examples.zip', selector: specific(params.APICURIO_PREPARE_BUILD_NUMBER)
                    }
                }

                stage('Copy strimzi artifacts - latest') {
                    when {
                        expression { !params.STRIMZI_PREPARE_BUILD_NUMBER }
                    }
                    steps {
                        copyArtifacts projectName: 'ocp-downstream-strimzi-prepare-job', target: "${DEBEZIUM_LOCATION}/strimzi" , filter: 'amq-streams-install-examples.zip', selector: lastSuccessful()
                    }
                }
                stage('Copy strimzi artifacts') {
                    when {
                        expression { params.STRIMZI_PREPARE_BUILD_NUMBER }
                    }
                    steps {
                        copyArtifacts projectName: 'ocp-downstream-strimzi-prepare-job', target: "${DEBEZIUM_LOCATION}/strimzi" , filter: 'amq-streams-install-examples.zip', selector: specific(params.STRIMZI_PREPARE_BUILD_NUMBER)
                    }
                }
            }
        }

        stage('Prepare project') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        file(credentialsId: "${PULL_SECRET}", variable: 'SECRET_PATH'),
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

                    # create operators
                    source ./jenkins-jobs/docker/debezium-testing-system/testsuite-helper.sh
                    clone_component --component strimzi --git-repository "${STRZ_GIT_REPOSITORY}" --git-branch "${STRZ_GIT_BRANCH}" --product-build "${PRODUCT_BUILD}" ;
                    sed -i 's/namespace: .*/namespace: '"${OCP_PROJECT_DEBEZIUM}"'/' strimzi/install/cluster-operator/*RoleBinding*.yaml ;
                    oc apply -f strimzi/install/cluster-operator/ -n "${OCP_PROJECT_DEBEZIUM}"  || true ;

                    if [ ${TEST_APICURIO_REGISTRY} == true ]; then
                      if [ -z "${APIC_GIT_REPOSITORY}" ]; then
                        APIC_GIT_REPOSITORY="https://github.com/Apicurio/apicurio-registry-operator.git" ;
                      fi

                      if [ -z "${APIC_GIT_BRANCH}" ]; then
                        APIC_GIT_BRANCH="master" ;
                      fi

                      if [ -z "${APICURIO_RESOURCE}" ] && [ "${PRODUCT_BUILD}" == false ]; then
                        APICURIO_RESOURCE="install/apicurio-registry-operator-1.1.0-dev.yaml"
                      elif [ -z "${APICURIO_RESOURCE}" ] && [ "${PRODUCT_BUILD}" == true ]; then
                        APICURIO_RESOURCE="install/install.yaml"
                      fi

                      clone_component --component apicurio --git-repository "${APIC_GIT_REPOSITORY}" --git-branch "${APIC_GIT_BRANCH}" --product-build "${PRODUCT_BUILD}" ;
                      sed -i "s/namespace: apicurio-registry-operator-namespace/namespace: ${OCP_PROJECT_REGISTRY}/" apicurio/install/*.yaml ;
                      oc apply -f "${SECRET_PATH}" -n "${OCP_PROJECT_REGISTRY}"
                      oc apply -f apicurio/${APICURIO_RESOURCE} -n "${OCP_PROJECT_REGISTRY}" || true ;
                    fi
                    '''
                }
            }
        }

        stage('Run testsuite') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        file(credentialsId: "${PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    sh '''
                    cd ${DEBEZIUM_LOCATION}
                    source ./${OCP_PROJECT_NAME}.ocp.env

                    GROUPS_ARG="!docker"
                    if [ ${TEST_APICURIO_REGISTRY} == false ]; then
                        GROUPS_ARG="${GROUPS_ARG} & !avro"
                    fi

                    FILENAME="testsuite-job"
                    PULL_SECRET_NAME=$(cat ${SECRET_PATH} | grep name | awk '{print $2;}')

                    cd ${WORKSPACE}/debezium
                    jenkins-jobs/docker/debezium-testing-system/deployment-template.sh --filename "${FILENAME}" \
                    --dbz-git-repository "${DBZ_GIT_REPOSITORY}" \
                    --dbz-git-branch "${DBZ_GIT_BRANCH}" \
                    --pull-secret-name "${PULL_SECRET_NAME}" \
                    --docker-tag "${DOCKER_TAG}" \
                    --project-name "${OCP_PROJECT_NAME}" \
                    --product-build "${PRODUCT_BUILD}" \
                    --strimzi-kc-build ${STRIMZI_KC_BUILD} \
                    --apicurio-version "${APICURIO_VERSION}" \
                    --kafka-version "${KAFKA_VERSION}" \
                    --groups-arg "${GROUPS_ARG}" \
                    --dbz-connect-image "${DBZ_CONNECT_IMAGE}" \
                    --artifact-server-image "${ARTIFACT_SERVER_IMAGE}" \
                    --apicurio-version "${APICURIO_VERSION}"

                    oc delete -f "${FILENAME}.yml" --ignore-not-found
                    oc create -f "${FILENAME}.yml"

                    for i in {1..100}; do
                        sleep 2
                        pod_name=$(oc get pods | tail -1 | awk '{print $1;}')
                        oc logs -f ${pod_name} && break
                    done

                    oc rsync ${pod_name}:/testsuite/debezium/debezium-testing/debezium-testing-system/target/failsafe-reports ./debezium-testing-system/target/failsafe-reports
                    archiveArtifacts '**/debezium-testing/debezium-testing-system/target/failsafe-reports/*.xml'

                    oc rsync ${pod_name}:/testsuite/debezium/target/failsafe-reports ./failsafe-reports
                    archiveArtifacts '**/target/failsafe-reports/*.xml'\
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
        }
    }
}
