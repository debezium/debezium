#!/bin/bash
SCRIPT_LOCATION=${HOME}/testsuite
cd "${SCRIPT_LOCATION}" || exit 1
{
  source "${HOME}/.sdkman/bin/sdkman-init.sh"

  set -x

  DEBEZIUM_LOCATION=${SCRIPT_LOCATION}/debezium

  mkdir "${DEBEZIUM_LOCATION}"

  SECRET_LOCATION="/root/testsuite/pull.yaml"


  # clone, compile debezium and run tests
  git clone --branch "${DBZ_GIT_BRANCH}" "${DBZ_GIT_REPOSITORY}"


  oc get secret -n "${DBZ_OCP_PROJECT_DEBEZIUM}-testsuite" "${DBZ_SECRET_NAME}" -o yaml | sed "s/namespace: .*//" | sed "s/uid: .*//" > "${SECRET_LOCATION}"

  pushd "${DEBEZIUM_LOCATION}" || exit 1

  ./mvnw install -Dquick


  if [[ -n "${DEBUG_PORT}" ]]; then
    DEBUG_OPTION=-Dmaven.failsafe.debug=\"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:${DEBUG_PORT}\"
    MVN_OPTIONS='install -pl debezium-testing/debezium-testing-system -PsystemITs,oracleITs '${DEBUG_OPTION}' -Docp.pull.secret.paths='${SECRET_LOCATION}' '$TESTSUITE_ARGUMENTS''
  else
    MVN_OPTIONS='install -pl debezium-testing/debezium-testing-system -PsystemITs,oracleITs -Docp.pull.secret.paths='${SECRET_LOCATION}' '$TESTSUITE_ARGUMENTS''
  fi


  eval './mvnw '$MVN_OPTIONS''


  pushd debezium-testing/debezium-testing-system/target/failsafe-reports || exit 1
  zip artifacts ./*.xml
  mv artifacts.zip "${SCRIPT_LOCATION}"
} 2>&1 | tee "${SCRIPT_LOCATION}/tmp_log"

# move to a location checked by a readiness probe to signal jenkins artefacts are ready
cd "${SCRIPT_LOCATION}" || exit 1
mv tmp_log testsuite_log
