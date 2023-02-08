#!/bin/bash

OPTS=$(getopt -o h: --long dbz-git-repository:,dbz-git-branch:,filename:,pull-secret-name:,docker-tag:,project-name:,product-build:,strimzi-kc-build:,dbz-connect-image:,artifact-server-image:,apicurio-version:,kafka-version:,groups-arg:,testsuite-log:,strimzi-channel:,apicurio-channel:,prepare-strimzi:,help  -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

echo "${OPTS[@]}"

while true; do
    case "$1" in
        --dbz-git-repository )    DBZ_GIT_REPOSITORY=$2;        shift 2;;
        --dbz-git-branch )        DBZ_GIT_BRANCH=$2;            shift 2;;
        --filename )              FILENAME=$2;                  shift 2;;
        --pull-secret-name )      PULL_SECRET_NAME=$2;          shift 2;;
        --docker-tag )            DOCKER_TAG=$2;                shift 2;;
        --project-name )          PROJECT_NAME=$2;              shift 2;;
        --product-build )         PRODUCT_BUILD=$2;             shift 2;;
        --strimzi-kc-build )      STRIMZI_KC_BUILD=$2;          shift 2;;
        --dbz-connect-image )     DBZ_CONNECT_IMAGE=$2;         shift 2;;
        --artifact-server-image ) ARTIFACT_SERVER_IMAGE=$2;     shift 2;;
        --apicurio-version )      APICURIO_VERSION=$2;          shift 2;;
        --kafka-version )         KAFKA_VERSION=$2;             shift 2;;
        --groups-arg )            GROUPS_ARG=$2;                shift 2;;
        --testsuite-log )         TESTSUITE_LOG=$2;             shift 2;;
        --strimzi-channel )       STRIMZI_OPERATOR_CHANNEL=$2;  shift 2;;
        --apicurio-channel )      APICURIO_OPERATOR_CHANNEL=$2; shift 2;;
        --prepare-strimzi )       PREPARE_STRIMZI=$2;           shift 2;;
        -h | --help )             PRINT_HELP=true=$2;           shift ;;
        -- ) shift; break ;;
        * ) break ;;
    esac
done

output="apiVersion: v1
kind: Pod
metadata:
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
        - name: DBZ_GIT_REPOSITORY
          value: \"${DBZ_GIT_REPOSITORY}\"
        - name: DBZ_GIT_BRANCH
          value: \"${DBZ_GIT_BRANCH}\"
        - name: DBZ_OCP_PROJECT_DEBEZIUM
          value: \"${PROJECT_NAME}\"
        - name: DBZ_SECRET_NAME
          value: \"${PULL_SECRET_NAME}\"
        - name: DBZ_TEST_WAIT_SCALE
          value: \"10\"
        - name: DBZ_PRODUCT_BUILD
          value: \"${PRODUCT_BUILD}\"
        - name: DBZ_STRIMZI_KC_BUILD
          value: \"${STRIMZI_KC_BUILD}\"
        - name: DBZ_CONNECT_IMAGE
          value: \"${DBZ_CONNECT_IMAGE}\"
        - name: DBZ_ARTIFACT_SERVER_IMAGE
          value: \"${ARTIFACT_SERVER_IMAGE}\"
        - name: DBZ_APICURIO_VERSION
          value: \"${APICURIO_VERSION}\"
        - name: DBZ_KAFKA_VERSION
          value: \"${KAFKA_VERSION}\"
        - name: STRIMZI_OPERATOR_CHANNEL
          value: \"${STRIMZI_OPERATOR_CHANNEL}\"
        - name: APICURIO_OPERATOR_CHANNEL
          value: \"${APICURIO_OPERATOR_CHANNEL}\"
        - name: DBZ_GROUPS_ARG
          value: \"${GROUPS_ARG}\"
        - name: DBZ_OCP_DELETE_PROJECTS
          value: \"true\"
        - name: PREPARE_STRIMZI
          value: \"${PREPARE_STRIMZI}\"
      readinessProbe:
        exec:
          command:
          - cat
          - ${TESTSUITE_LOG}
  triggers:
    - type: \"ConfigChange\"
  paused: false
  revisionHistoryLimit: 2
  minReadySeconds: 0
"

printf "%s" "${output}" >> "${FILENAME}"
