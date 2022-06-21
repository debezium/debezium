#!/bin/bash

OPTS=$(getopt -o h: --long filename:,pull-secret-name:,docker-tag:,project-name:,product-build:,strimzi-kc-build:,dbz-connect-image:,artifact-server-image:,apicurio-version:,groups-arg:,strz-git-repository:,strz-git-branch:,strz-downstream-url:,apic-git-repository:,apic-git-branch:,apic-downstream-url:,help  -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

echo "${OPTS[@]}"

while true; do
    case "$1" in
        --filename )              FILENAME=$2;                shift 2;;
        --pull-secret-name )      PULL_SECRET_NAME=$2;        shift 2;;
        --docker-tag )            DOCKER_TAG=$2;              shift 2;;
        --project-name )          PROJECT_NAME=$2;            shift 2;;
        --product-build )         PRODUCT_BUILD=$2;           shift 2;;
        --strimzi-kc-build )      STRIMZI_KC_BUILD=$2;        shift 2;;
        --dbz-connect-image )     DBZ_CONNECT_IMAGE=$2;       shift 2;;
        --artifact-server-image ) ARTIFACT_SERVER_IMAGE=$2;   shift 2;;
        --apicurio-version )      APICURIO_VERSION=$2;        shift 2;;
        --groups-arg )            GROUPS_ARG=$2;              shift 2;;
        --strz-git-repository )   STRZ_GIT_REPOSITORY=$2;     shift 2;;
        --strz-git-branch )       STRZ_GIT_BRANCH=$2;         shift 2;;
        --strz-downstream-url )   STRZ_DOWNSTREAM_URL=$2;     shift 2;;
        --apic-git-repository )   APIC_GIT_REPOSITORY=$2;     shift 2;;
        --apic-git-branch )       APIC_GIT_BRANCH=$2;         shift 2;;
        --apic-downstream-url )   APIC_DOWNSTREAM_URL=$2;     shift 2;;
        -h | --help )             PRINT_HELP=true=$2;         shift ;;
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
            - name: DBZ_OCP_PROJECT_DEBEZIUM
              value: \"${PROJECT_NAME}\"
            - name: DBZ_OCP_PROJECT_DB2
              value: \"${PROJECT_NAME}-db2\"
            - name: DBZ_OCP_PROJECT_MONGO
              value: \"${PROJECT_NAME}-mongo\"
            - name: DBZ_OCP_PROJECT_MYSQL
              value: \"${PROJECT_NAME}-mysql\"
            - name: DBZ_OCP_PROJECT_ORACLE
              value: \"${PROJECT_NAME}-oracle\"
            - name: DBZ_OCP_PROJECT_POSTGRESQL
              value: \"${PROJECT_NAME}-postgresql\"
            - name: DBZ_OCP_PROJECT_SQLSERVER
              value: \"${PROJECT_NAME}-sqlserver\"
            - name: DBZ_OCP_PROJECT_REGISTRY
              value: \"${PROJECT_NAME}-registry\"
            - name: DBZ_SECRET_PATH
              value: \"/testsuite/secret.yml\"
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
            - name: DBZ_GROUPS_ARG
              value: \"${GROUPS_ARG}\"
            - name: DBZ_OCP_DELETE_PROJECTS
              value: \"true\"
            - name: STRZ_GIT_REPOSITORY
              value: \"${STRZ_GIT_REPOSITORY}\"
            - name: STRZ_GIT_BRANCH
              value: \"${STRZ_GIT_BRANCH}\"
            - name: STRZ_DOWNSTREAM_URL
              value: \"${STRZ_DOWNSTREAM_URL}\"
            - name: APIC_GIT_REPOSITORY
              value: \"${APIC_GIT_REPOSITORY}\"
            - name: APIC_GIT_BRANCH
              value: \"${APIC_GIT_BRANCH}\"
            - name: APIC_DOWNSTREAM_URL
              value: \"${APIC_DOWNSTREAM_URL}\"
  triggers:
    - type: \"ConfigChange\"
  paused: false
  revisionHistoryLimit: 2
  minReadySeconds: 0
"


printf "%s" "${output}" >> "${FILENAME}.yml"
