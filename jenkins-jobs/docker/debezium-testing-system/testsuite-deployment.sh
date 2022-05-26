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

#prepare ocp, run tests
${OCP_PROJECTS} --project "${OCP_PROJECT_DEBEZIUM}" --create

clone_strimzi --strimzi-repository "${STRZ_GIT_REPOSITORY}" --strimzi-branch "${STRZ_GIT_BRANCH}" --product-build "${PRODUCT_BUILD}" --strimzi-downstream "${OCP_STRIMZI_DOWNSTREAM_URL}";
sed -i 's/namespace: .*/namespace: '"${OCP_PROJECT_DEBEZIUM}"'/' strimzi/install/cluster-operator/*RoleBinding*.yaml ;

oc create -f strimzi/install/cluster-operator/ -n "${OCP_PROJECT_DEBEZIUM}" ;

pushd ${DEBEZIUM_LOCATION} || exit 1;

if [ -z "${TEST_VERSION_KAFKA}" ]; then
  TEST_PROPERTIES="";
else 
  TEST_PROPERTIES="-Dversion.kafka=${TEST_VERSION_KAFKA}" ;
fi 

if [ -n "${DBZ_CONNECT_IMAGE}" ]; then
  TEST_PROPERTIES="$TEST_PROPERTIES -Dimage.kc=${DBZ_CONNECT_IMAGE}" ;
fi

mvn install -pl debezium-testing/debezium-testing-system -PsystemITs \
                    -Docp.project.debezium="${OCP_PROJECT_DEBEZIUM}" \
                    -Docp.project.mysql="${OCP_PROJECT_MYSQL}" \
                    -Docp.project.oracle="${OCP_PROJECT_ORACLE}" \
                    -Docp.project.postgresql="${OCP_PROJECT_POSTGRESQL}" \
                    -Docp.project.sqlserver="${OCP_PROJECT_SQLSERVER}" \
                    -Docp.project.mongo="${OCP_PROJECT_MONGO}" \
                    -Docp.project.db2="${OCP_PROJECT_DB2}" \
                    -Docp.pull.secret.paths="${SECRET_PATH}" \
                    -Dtest.wait.scale="${TEST_WAIT_SCALE}" \
                    -Dtest.strimzi.kc.build=${PRODUCT_BUILD} \
                    -Dimage.kc="${DBZ_CONNECT_IMAGE}" \
                    -Dimage.as="${ARTIFACT_SERVER_IMAGE}" \
                    -Dgroups="${GROUPS}"

popd || exit 1;

cp debezium/debezium-testing/debezium-testing-system/target/failsafe-reports/*.xml /testsuite/logs

if [ "${DELETE_PROJECTS}" = true ] ;
then
  ${OCP_PROJECTS} --project "${OCP_PROJECT_DEBEZIUM}" --delete
fi ;
