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

                    DBZ_GROUPS_ARG="!docker"
                    if [ ${TEST_APICURIO_REGISTRY} == false ]; then
                        DBZ_GROUPS_ARG="${DBZ_GROUPS_ARG} & !avro"
                    fi

                    JOB_DESC_FILE="testsuite-job.yml"
                    PULL_SECRET_NAME=$(cat ${SECRET_PATH} | grep name | awk '{print $2;}')

                    printf "%s" "apiVersion: batch/v1
kind: Job
metadata:
  name: \\"testsuite\\"
spec:
  template:
    metadata:
      labels:
        name: \\"testsuite\\"
    spec:
      restartPolicy: Never
      imagePullSecrets:
        - name: ${PULL_SECRET_NAME}
      containers:
        - name: \\"dbz-testing-system\\"
          image: \\"quay.io/rh_integration/dbz-testing-system:${DOCKER_TAG}\\"
          imagePullPolicy: Always
          ports:
            - containerPort: 8080
              protocol: \\"TCP\\"
          env:
            - name: DBZ_GIT_REPOSITORY
              value: \\"${DBZ_GIT_REPOSITORY}\\"
            - name: DBZ_GIT_BRANCH
              value: \\"${DBZ_GIT_BRANCH}\\"
            - name: DBZ_OCP_PROJECT_DEBEZIUM
              value: \\"${OCP_PROJECT_NAME}\\"
            - name: DBZ_SECRET_NAME
              value: \\"${PULL_SECRET_NAME}\\"
            - name: DBZ_TEST_WAIT_SCALE
              value: \\"10\\"
            - name: DBZ_PRODUCT_BUILD
              value: \\"${PRODUCT_BUILD}\\"
            - name: DBZ_STRIMZI_KC_BUILD
              value: \\"${STRIMZI_KC_BUILD}\\"
            - name: DBZ_CONNECT_IMAGE
              value: \\"${DBZ_CONNECT_IMAGE}\\"
            - name: DBZ_ARTIFACT_SERVER_IMAGE
              value: \\"${ARTIFACT_SERVER_IMAGE}\\"
            - name: DBZ_APICURIO_VERSION
              value: \\"${APICURIO_VERSION}\\"
            - name: DBZ_KAFKA_VERSION
              value: \\"${KAFKA_VERSION}\\"
            - name: DBZ_GROUPS_ARG
              value: \\"${DBZ_GROUPS_ARG}\\"
            - name: DBZ_OCP_DELETE_PROJECTS
              value: \\"true\\"
  triggers:
    - type: \\"ConfigChange\\"
  paused: false
  revisionHistoryLimit: 2
  minReadySeconds: 0
" >> "${JOB_DESC_FILE}"

                    oc delete -f "${JOB_DESC_FILE}" --ignore-not-found
                    oc create -f "${JOB_DESC_FILE}"

                    for i in {1..100}; do
                        sleep 2
                        pod_name=$(oc get pods | tail -1 | awk '{print $1;}')
                        oc logs -f ${pod_name} && break
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
        }
    }
}
