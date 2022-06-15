#!/bin/bash

source /root/.sdkman/bin/sdkman-init.sh
source /testsuite/library.sh

DEBEZIUM_LOCATION="/testsuite/debezium"
OCP_PROJECTS="${DEBEZIUM_LOCATION}/jenkins-jobs/scripts/ocp-projects.sh"
SECRET_PATH=/testsuite/secret.yml

if [ ! -f "${SECRET_PATH}" ]; then
    echo "secret ${SECRET_PATH} does not  exist!"
    exit 1
fi

# TODO remove git pull and rebuild once the development is done
git -C /testsuite/debezium stash
git -C /testsuite/debezium pull --rebase origin DBZ-5165
git -C /testsuite/debezium log -1

mvn clean install -DskipTests -DskipITs -f /testsuite/debezium/pom.xml

# create projects
${OCP_PROJECTS} --project "${OCP_PROJECT_DEBEZIUM}" --create

# prepare strimzi
# TODO remove the defaults once it's being ran from jenkins?
if [ -z "${STRZ_GIT_REPOSITORY}" ]; then
  STRZ_GIT_REPOSITORY="https://github.com/strimzi/strimzi-kafka-operator.git" ;
fi

if [ -z "${STRZ_GIT_BRANCH}" ]; then
  STRZ_GIT_BRANCH="main" ;
fi

clone_component --component strimzi --git-repository "${STRZ_GIT_REPOSITORY}" --git-branch "${STRZ_GIT_BRANCH}" --product-build "${PRODUCT_BUILD}" --downstream-url "${STRZ_DOWNSTREAM_URL}" ;
sed -i 's/namespace: .*/namespace: '"${OCP_PROJECT_DEBEZIUM}"'/' strimzi/install/cluster-operator/*RoleBinding*.yaml ;
oc create -f strimzi/install/cluster-operator/ -n "${OCP_PROJECT_DEBEZIUM}" ;

# prepare apicurio if not disabled
AVRO_PATTERN='.*!avro.*'
if [[ ! ${GROUPS_ARG} =~ ${AVRO_PATTERN} ]]; then

  # TODO remove the defaults once it's being ran from jenkins?
  if [ -z "${APIC_GIT_REPOSITORY}" ]; then
    APIC_GIT_REPOSITORY="https://github.com/Apicurio/apicurio-registry-operator.git" ;
  fi

  if [ -z "${APIC_GIT_BRANCH}" ]; then
    APIC_GIT_BRANCH="master" ;
  fi

  if [ -z "${APICURIO_RESOURCE}" ]; then
    APICURIO_RESOURCE="install/apicurio-registry-operator-1.1.0-dev.yaml"
  fi

  clone_component --component apicurio --git-repository "${APIC_GIT_REPOSITORY}" --git-branch "${APIC_GIT_BRANCH}" --product-build "${PRODUCT_BUILD}" --downstream-url "${APIC_DOWNSTREAM_URL}" ;
  sed -i "s/namespace: apicurio-registry-operator-namespace/namespace: ${OCP_PROJECT_REGISTRY}/" apicurio/install/*.yaml ;
  oc create -f apicurio/${APICURIO_RESOURCE} -n "${OCP_PROJECT_REGISTRY}" ;
fi

pushd ${DEBEZIUM_LOCATION} || exit 1;

oc project "${OCP_PROJECT_DEBEZIUM}"
mvn install -pl debezium-testing/debezium-testing-system -PsystemITs,oracleITs \
                    -Docp.project.debezium="${OCP_PROJECT_DEBEZIUM}" \
                    -Docp.project.db2="${OCP_PROJECT_DB2}" \
                    -Docp.project.mongo="${OCP_PROJECT_MONGO}" \
                    -Docp.project.mysql="${OCP_PROJECT_MYSQL}" \
                    -Docp.project.oracle="${OCP_PROJECT_ORACLE}" \
                    -Docp.project.postgresql="${OCP_PROJECT_POSTGRESQL}" \
                    -Docp.project.sqlserver="${OCP_PROJECT_SQLSERVER}" \
                    -Docp.project.registry="${OCP_PROJECT_REGISTRY}" \
                    -Docp.pull.secret.paths="${SECRET_PATH}" \
                    -Dtest.wait.scale="${TEST_WAIT_SCALE}" \
                    -Dtest.strimzi.kc.build="${STRIMZI_KC_BUILD}" \
                    -Dimage.kc="${DBZ_CONNECT_IMAGE}" \
                    -Dimage.as="${ARTIFACT_SERVER_IMAGE}" \
                    -Das.apicurio.version="${APICURIO_VERSION}" \
                    -Dgroups="${GROUPS_ARG}"

popd || exit 1;

cp debezium/debezium-testing/debezium-testing-system/target/failsafe-reports/*.xml /testsuite/logs

if [ "${DELETE_PROJECTS}" = true ] ;
then
  ${OCP_PROJECTS} --project "${OCP_PROJECT_DEBEZIUM}" --delete
fi ;
