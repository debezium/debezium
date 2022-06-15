#!/bin/bash

PULL_SECRET_NAME="aswerf"

OPTS=$(getopt -o h: --long pull-secret-name:,docker-tag:,project-name:,product-build:,strimzi-kc-build:,dbz-connect-image:,artifact-server-image:,apicurio-version:,groups-arg:,help  -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

echo "${OPTS[@]}"

while true; do
    case "$1" in
        --pull-secret-name )      PULL_SECRET_NAME=$2;        shift 2;;
        --docker-tag )            DOCKER_TAG=$2;              shift 2;;
        --project-name )          PROJECT_NAME=$2;            shift 2;;
        --product-build )         PRODUCT_BUILD=$2;           shift 2;;
        --strimzi-kc-build )      STRIMZI_KC_BUILD=$2;        shift 2;;
        --dbz-connect-image )     DBZ_CONNECT_IMAGE=$2;       shift 2;;
        --artifact-server-image ) ARTIFACT_SERVER_IMAGE=$2;   shift 2;;
        --apicurio-version )      APICURIO_VERSION=$2;        shift 2;;
        --groups-arg )            GROUPS_ARG=$2;              shift 2;;
        -h | --help )             PRINT_HELP=true             shift ;;
        -- ) shift; break ;;
        * ) break ;;
    esac
done

output="apiVersion: batch/v1
kind: Job
metadata:
  name: \"testsuite\"
spec:
  template:
    metadata:
      labels:
        name: \"testsuite\"
    spec:
      restartPolicy: Never
      imagePullSecrets:
        - name: ${PULL_SECRET_NAME}
      containers:
        - name: \"dbz-testing-system\"
          image: \"quay.io/rh_integration/dbz-testing-system:${DOCKER_TAG}\"
          imagePullPolicy: Always
          ports:
            - containerPort: 8080
              protocol: \"TCP\"
          env:
            - name: OCP_PROJECT_DEBEZIUM
              value: \"${PROJECT_NAME}\"
            - name: OCP_PROJECT_DB2
              value: \"${PROJECT_NAME}-db2\"
            - name: OCP_PROJECT_MONGO
              value: \"${PROJECT_NAME}-mongo\"
            - name: OCP_PROJECT_MYSQL
              value: \"${PROJECT_NAME}-mysql\"
            - name: OCP_PROJECT_ORACLE
              value: \"${PROJECT_NAME}-oracle\"
            - name: OCP_PROJECT_POSTGRESQL
              value: \"${PROJECT_NAME}-postgresql\"
            - name: OCP_PROJECT_SQLSERVER
              value: \"${PROJECT_NAME}-sqlserver\"
            - name: OCP_PROJECT_REGISTRY
              value: \"${PROJECT_NAME}-registry\"
            - name: SECRET_PATH
              value: \"/testsuite/secret.yml\"
            - name: TEST_WAIT_SCALE
              value: \"10\"
            - name: PRODUCT_BUILD
              value: \"${PRODUCT_BUILD}\"
            - name: STRIMZI_KC_BUILD
              value: \"${STRIMZI_KC_BUILD}\"
            - name: DBZ_CONNECT_IMAGE
              value: \"${DBZ_CONNECT_IMAGE}\"
            - name: ARTIFACT_SERVER_IMAGE
              value: \"${ARTIFACT_SERVER_IMAGE}\"
            - name: APICURIO_VERSION
              value: \"${APICURIO_VERSION}\"
            - name: GROUPS_ARG
              value: \"${GROUPS_ARG}\"
            - name: DELETE_PROJECTS
              value: \"true\"
  triggers:
    - type: \"ConfigChange\"
  paused: false
  revisionHistoryLimit: 2
  minReadySeconds: 0
"


printf "%s" "$output" >> test-job-deployment.yml