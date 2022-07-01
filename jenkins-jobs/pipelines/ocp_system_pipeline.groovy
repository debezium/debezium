pipeline {
    agent {
        label 'Slave'
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

        stage('Prepare project') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        file(credentialsId: "${PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    sh '''
                    oc login -u "${OCP_USERNAME}" -p "${OCP_PASSWORD}" --insecure-skip-tls-verify=true "${OCP_URL}"
                    oc new-project "${OCP_PROJECT_NAME}-testsuite" || oc project "${OCP_PROJECT_NAME}-testsuite"
                    oc adm policy add-cluster-role-to-user cluster-admin "system:serviceaccount:${OCP_PROJECT_NAME}-testsuite:default"
                    oc apply -f "${SECRET_PATH}"
                    '''
                }
            }
        }

        stage('Run tests') {
            steps {
                withCredentials([
                        file(credentialsId: "${PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    sh '''
                    GROUPS_ARG="!docker"
                    if [ ${TEST_APICURIO_REGISTRY} == false ]; then
                        GROUPS_ARG="${GROUPS_ARG} & !avro"
                    fi

                    FILENAME="testsuite-job"
                    SECRET_NAME=$(cat ${SECRET_PATH} | grep name | awk '{print $2;}')

                    cd ${WORKSPACE}/debezium
                    jenkins-jobs/docker/debezium-testing-system/deployment-template.sh --filename "${FILENAME}" \
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
                    --apicurio-version "${APICURIO_VERSION}" \
                    --strz-git-repository "${STRZ_GIT_REPOSITORY}" \
                    --strz-git-branch "${STRZ_GIT_BRANCH}" \
                    --apic-git-repository "${APIC_GIT_REPOSITORY}" \
                    --apic-git-branch "${APIC_GIT_BRANCH}"
                    oc delete -f "${FILENAME}.yml" --ignore-not-found
                    oc create -f "${FILENAME}.yml"
                    '''
                }
            }
        }
    }
}
