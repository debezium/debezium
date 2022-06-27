#!/bin/bash

source /root/.sdkman/bin/sdkman-init.sh
source /testsuite/library.sh

DEBEZIUM_LOCATION="/testsuite/debezium"
OCP_PROJECTS="${DEBEZIUM_LOCATION}/jenkins-jobs/scripts/ocp-projects.sh"

if [ -z "${DBZ_OCP_PROJECT_DEBEZIUM}" ]; then
    echo "project name is required. Please set DBZ_OCP_PROJECT_DEBEZIUM!"
    exit 1
fi

# TODO remove git pull and rebuild once the development is done
#git -C /testsuite/debezium stash
#git -C /testsuite/debezium pull --rebase origin DBZ-5165
#git -C /testsuite/debezium log -1
##
#mvn install -DskipTests -DskipITs -f /testsuite/debezium/pom.xml

# create projects
${OCP_PROJECTS} --project "${DBZ_OCP_PROJECT_DEBEZIUM}" --create

# copy parent secret to debezium projects
PARENT_SECRET=parent_secret.yml
for project in ${DBZ_OCP_PROJECT_DEBEZIUM} ${DBZ_OCP_PROJECT_REGISTRY} ${DBZ_OCP_PROJECT_DB2} ${DBZ_OCP_PROJECT_ORACLE} ; do
  oc get secret -n "${DBZ_OCP_PROJECT_DEBEZIUM}-parent" "${DBZ_SECRET_NAME}" -o yaml | sed "s/namespace: .*/namespace: ${project}/" >> ${PARENT_SECRET}
  oc create -n "${project}" -f ${PARENT_SECRET}
  rm ${PARENT_SECRET}
done

# prepare strimzi
clone_component --component strimzi --git-repository "${STRZ_GIT_REPOSITORY}" --git-branch "${STRZ_GIT_BRANCH}" --product-build "${DBZ_PRODUCT_BUILD}" --downstream-url "${STRZ_DOWNSTREAM_URL}" ;
sed -i 's/namespace: .*/namespace: '"${DBZ_OCP_PROJECT_DEBEZIUM}"'/' strimzi/install/cluster-operator/*RoleBinding*.yaml ;
oc create -f strimzi/install/cluster-operator/ -n "${DBZ_OCP_PROJECT_DEBEZIUM}" ;

# prepare apicurio if not disabled
AVRO_PATTERN='.*!avro.*'
if [[ ! ${DBZ_GROUPS_ARG} =~ ${AVRO_PATTERN} ]]; then
  if [ -z "${APIC_GIT_REPOSITORY}" ]; then
    APIC_GIT_REPOSITORY="https://github.com/Apicurio/apicurio-registry-operator.git" ;
  fi

  if [ -z "${APIC_GIT_BRANCH}" ]; then
    APIC_GIT_BRANCH="master" ;
  fi

  if [ -z "${APICURIO_RESOURCE}" ] && [ "${DBZ_PRODUCT_BUILD}" == false ]; then
    APICURIO_RESOURCE="install/apicurio-registry-operator-1.1.0-dev.yaml"
  elif [ -z "${APICURIO_RESOURCE}" ] && [ "${DBZ_PRODUCT_BUILD}" == true ]; then
    APICURIO_RESOURCE="install/install.yaml"
  fi

  clone_component --component apicurio --git-repository "${APIC_GIT_REPOSITORY}" --git-branch "${APIC_GIT_BRANCH}" --product-build "${DBZ_PRODUCT_BUILD}" --downstream-url "${APIC_DOWNSTREAM_URL}" ;
  sed -i "s/namespace: apicurio-registry-operator-namespace/namespace: ${DBZ_OCP_PROJECT_REGISTRY}/" apicurio/install/*.yaml ;
  oc create -f apicurio/${APICURIO_RESOURCE} -n "${DBZ_OCP_PROJECT_REGISTRY}" ;
fi

pushd ${DEBEZIUM_LOCATION} || exit 1;

if [ "${DBZ_PRODUCT_BUILD}" == true ] ; then
  MVN_PRODUCT_BUILD="-Pproduct"
fi

mvn install -pl debezium-testing/debezium-testing-system -PsystemITs,oracleITs \
                    ${MVN_PRODUCT_BUILD} \
                    -Docp.project.debezium="${DBZ_OCP_PROJECT_DEBEZIUM}" \
                    -Docp.project.db2="${DBZ_OCP_PROJECT_DB2}" \
                    -Docp.project.mongo="${DBZ_OCP_PROJECT_MONGO}" \
                    -Docp.project.mysql="${DBZ_OCP_PROJECT_MYSQL}" \
                    -Docp.project.oracle="${DBZ_OCP_PROJECT_ORACLE}" \
                    -Docp.project.postgresql="${DBZ_OCP_PROJECT_POSTGRESQL}" \
                    -Docp.project.sqlserver="${DBZ_OCP_PROJECT_SQLSERVER}" \
                    -Docp.project.registry="${DBZ_OCP_PROJECT_REGISTRY}" \
                    -Docp.pull.secret.name="${DBZ_SECRET_NAME}" \
                    -Dtest.wait.scale="${DBZ_TEST_WAIT_SCALE}" \
                    -Dtest.strimzi.kc.build="${DBZ_STRIMZI_KC_BUILD}" \
                    -Dimage.kc="${DBZ_CONNECT_IMAGE}" \
                    -Dimage.as="${DBZ_ARTIFACT_SERVER_IMAGE}" \
                    -Das.apicurio.version="${DBZ_APICURIO_VERSION}" \
                    -Dgroups="${DBZ_GROUPS_ARG}"

popd || exit 1;

if [ "${DBZ_OCP_DELETE_PROJECTS}" = true ] ;
then
  ${OCP_PROJECTS} --project "${DBZ_OCP_PROJECT_DEBEZIUM}" --delete
fi ;
