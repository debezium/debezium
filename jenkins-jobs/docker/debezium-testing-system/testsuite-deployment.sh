#!/bin/bash

source /testsuite/library.sh

clone_repositories --dbz-repository ${DBZ_GIT_REPOSITORY} \
 --dbz-branch ${DBZ_GIT_BRANCH} --strimzi-repository ${STRZ_GIT_REPOSITORY} --strimzi-branch ${STRZ_GIT_BRANCH} --product-build ${PRODUCT_BUILD} --strimzi-downstream ${OCP_STRIMZI_DOWNSTREAM_URL};

oc login "${OCP_URL}" -u "${OCP_USERNAME}" -p "${OCP_PASSWORD}" --insecure-skip-tls-verify=true >/dev/null ;

create_projects "${OCP_PROJECT_DEBEZIUM}" "${OCP_PROJECT_MYSQL}" "${OCP_PROJECT_POSTGRESQL}" "${OCP_PROJECT_SQLSERVER}" "${OCP_PROJECT_MONGO}" "${OCP_PROJECT_DB2}";

sed -i 's/namespace: .*/namespace: '"${OCP_PROJECT_DEBEZIUM}"'/' strimzi/install/cluster-operator/*RoleBinding*.yaml ;
oc create -f strimzi/install/cluster-operator/ -n ${OCP_PROJECT_DEBEZIUM} ;  

oc project ${OCP_PROJECT_SQLSERVER} && oc adm policy add-scc-to-user anyuid system:serviceaccount:${OCP_PROJECT_SQLSERVER}:default ; 
oc project ${OCP_PROJECT_MONGO} && oc adm policy add-scc-to-user anyuid system:serviceaccount:${OCP_PROJECT_MONGO}:default ; 
oc project ${OCP_PROJECT_DB2} && oc adm policy add-scc-to-user anyuid system:serviceaccount:${OCP_PROJECT_DB2}:default && oc adm policy add-scc-to-user privileged system:serviceaccount:${OCP_PROJECT_DB2}:default ; 

pushd debezium ;
./mvnw clean install -DskipTests -DskipITs ;

if [ -z ${TEST_VERSION_KAFKA} ] ;
then 
    TEST_PROPERTIES="";
else 
    TEST_PROPERTIES="-Dversion.kafka=${TEST_VERSION_KAFKA}" ;
fi 

if [ ! -z ${DBZ_CONNECT_IMAGE} ] ;
then 
    TEST_PROPERTIES="$TEST_PROPERTIES -Dimage.fullname=${DBZ_CONNECT_IMAGE}" ;
fi 

./mvnw install -pl debezium-testing/debezium-testing-system -PopenshiftITs \
                    -Dtest.ocp.username="${OCP_USERNAME}" \
                    -Dtest.ocp.password="${OCP_PASSWORD}" \
                    -Dtest.ocp.url="${OCP_URL}" \
                    -Dtest.ocp.project.debezium="${OCP_PROJECT_DEBEZIUM}" \
                    -Dtest.ocp.project.mysql="${OCP_PROJECT_MYSQL}" \
                    -Dtest.ocp.project.postgresql="${OCP_PROJECT_POSTGRESQL}" \
                    -Dtest.ocp.project.sqlserver="${OCP_PROJECT_SQLSERVER}" \
                    -Dtest.ocp.project.mongo="${OCP_PROJECT_MONGO}" \
                    -Dtest.ocp.project.db2="${OCP_PROJECT_DB2}" \
                    -Dtest.ocp.pull.secret.paths="${SECRET_PATH}" \
                    -Dtest.wait.scale="${TEST_WAIT_SCALE}" \
                    -Dtest.avro.serialisation="${TEST_APICURIO_REGISTRY}" \
                    "${TEST_PROPERTIES}" ;   

popd;

cp debezium/debezium-testing/debezium-testing-system/target/failsafe-reports/*.xml /testsuite/logs

if [ "${DELETE_PROJECTS}" = true ] ;
then 
    delete_projects "${OCP_PROJECT_DEBEZIUM}" "${OCP_PROJECT_MYSQL}" "${OCP_PROJECT_POSTGRESQL}" "${OCP_PROJECT_SQLSERVER}" "${OCP_PROJECT_MONGO}" "${OCP_PROJECT_DB2}";
fi ;
