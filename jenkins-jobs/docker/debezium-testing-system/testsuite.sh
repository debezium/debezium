#!/bin/bash
{
  source /root/.sdkman/bin/sdkman-init.sh

  set -x

  DEBEZIUM_LOCATION="/testsuite/debezium"

  # copy secret to debezium project
  TESTSUITE_SECRET=/testsuite/testsuite_secret.yml
  oc get secret -n "${DBZ_OCP_PROJECT_DEBEZIUM}-testsuite" "${DBZ_SECRET_NAME}" -o yaml | sed "s/namespace: .*//" | sed "s/uid: .*//" > ${TESTSUITE_SECRET}

  mkdir ${DEBEZIUM_LOCATION}
  pushd /testsuite || exit 1;

  OPTIONAL_ARGS=()
  if [ "${DBZ_PRODUCT_BUILD}" == true ] ; then
    OPTIONAL_ARGS+=("-Pproduct")
  fi

  if [ -n "${DBZ_KAFKA_VERSION}" ] ; then
    OPTIONAL_ARGS+=("-Dversion.kafka=${DBZ_KAFKA_VERSION}")
  fi

  # clone, compile debezium and run tests
  git clone --branch "${DBZ_GIT_BRANCH}" "${DBZ_GIT_REPOSITORY}"
  pushd debezium || exit 1

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
                      "${OPTIONAL_ARGS[@]}"

  pushd debezium-testing/debezium-testing-system/target || exit 1
  zip artifacts failsafe-reports/*.xml
  mv  artifacts.zip /testsuite
  popd || exit 1
} 2>&1 | tee /tmp/testsuite_log

# move from tmp to expected readiness file
cp /tmp/testsuite_log "${TESTSUITE_LOG}"

# keep alive until test results and logs are copied and pod is killed by jenkins job.
while true; do
  echo "Waiting to die."
  sleep 60
done
