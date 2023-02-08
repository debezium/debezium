#!/bin/bash
SCRIPT_LOCATION=${HOME}/testsuite
cd "${SCRIPT_LOCATION}" || exit 1
{
  source "${HOME}/.sdkman/bin/sdkman-init.sh"

  set -x

  DEBEZIUM_LOCATION=${SCRIPT_LOCATION}/debezium

  # copy secret to debezium project
  TESTSUITE_SECRET=${PWD}/testsuite_secret.yml
  oc get secret -n "${DBZ_OCP_PROJECT_DEBEZIUM}-testsuite" "${DBZ_SECRET_NAME}" -o yaml | sed "s/namespace: .*//" | sed "s/uid: .*//" > "${TESTSUITE_SECRET}"

  mkdir "${DEBEZIUM_LOCATION}"

  OPTIONAL_ARGS=()
  if [ "${DBZ_PRODUCT_BUILD}" == true ] ; then
    OPTIONAL_ARGS+=("-Pproduct")
  fi

  if [ -n "${DBZ_KAFKA_VERSION}" ] ; then
    OPTIONAL_ARGS+=("-Dversion.kafka=${DBZ_KAFKA_VERSION}")
  fi

  if [ -n "${STRIMZI_OPERATOR_CHANNEL}" ] ; then
    OPTIONAL_ARGS+=("-Dtest.strimzi.operator.channel=${STRIMZI_OPERATOR_CHANNEL}")
  fi

  if [ -n "${APICURIO_OPERATOR_CHANNEL}" ] ; then
    OPTIONAL_ARGS+=("-Dtest.apicurio.operator.channel=${APICURIO_OPERATOR_CHANNEL}")
  fi

  # clone, compile debezium and run tests
  git clone --branch "${DBZ_GIT_BRANCH}" "${DBZ_GIT_REPOSITORY}"
  pushd "${DEBEZIUM_LOCATION}" || exit 1

  ./mvnw install -DskipTests -DskipITs

  ./mvnw install -pl debezium-testing/debezium-testing-system -PsystemITs,oracleITs \
                      -Docp.project.debezium="${DBZ_OCP_PROJECT_DEBEZIUM}" \
                      -Docp.pull.secret.paths="${TESTSUITE_SECRET}" \
                      -Dtest.wait.scale="${DBZ_TEST_WAIT_SCALE}" \
                      -Dtest.strimzi.kc.build="${DBZ_STRIMZI_KC_BUILD}" \
                      -Dimage.kc="${DBZ_CONNECT_IMAGE}" \
                      -Dimage.as="${DBZ_ARTIFACT_SERVER_IMAGE}" \
                      -Das.apicurio.version="${DBZ_APICURIO_VERSION}" \
                      -Dgroups="${DBZ_GROUPS_ARG}" \
                      -Dprepare.strimzi="${PREPARE_STRIMZI}" \
                      "${OPTIONAL_ARGS[@]}"

  pushd debezium-testing/debezium-testing-system/target/failsafe-reports || exit 1
  zip artifacts ./*.xml
  mv artifacts.zip "${SCRIPT_LOCATION}"
} 2>&1 | tee "${SCRIPT_LOCATION}/tmp_log"

# move to a location checked by a readiness probe to signal jenkins artefacts are ready
cd "${SCRIPT_LOCATION}" || exit 1
mv tmp_log testsuite_log

# keep alive until test results and logs are copied and pod is killed by jenkins job.
while true; do
  echo "Waiting to die."
  sleep 60
done
