#! /usr/bin/env bash
set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DOCKER_FILE=${DIR}/../docker/Dockerfile.Strimzi
PLUGIN_DIR="plugins"

OPTS=$(getopt -o d:f:r:o:t:a: --long dir:,dockerfile:,registry:,organisation:,tags:,auto-tag:,kc-base-tag:,kafka-version:,apicurio-version:,dest-login:,dest-pass:,img-output:,oracle-included:,maven-repo: -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

while true; do
  case "$1" in
    -d | --dir )                BUILD_DIR=$2;
                                PLUGIN_DIR=${BUILD_DIR}/plugins;    shift; shift ;;
    -f | --dockerfile )         DOCKER_FILE=$2;                     shift; shift ;;
    -r | --registry )           REGISTRY=$2;                        shift; shift ;;
    -o | --organisation )       ORGANISATION=$2;                    shift; shift ;;
    -t | --tags )               TAGS=$2;                            shift; shift ;;
    -a | --auto-tag )           AUTO_TAG=$2;                        shift; shift ;;
    --kc-base-tag )             CONNECT_BASE_TAG=$2;                shift; shift ;;
    --kafka-version )           KAFKA_VERSION=$2;                   shift; shift ;;
    --apicurio-version )        APICURIO_VERSION=$2;                shift; shift ;;
    --dest-login )              DEST_LOGIN=$2;                      shift; shift ;;
    --dest-pass )               DEST_PASS=$2;                       shift; shift ;;
    --img-output )              IMAGE_OUTPUT_FILE=$2;               shift; shift ;;
    --oracle-included )         ORACLE=$2;                          shift; shift ;;
    --maven-repo )              MAVEN_REPO=$2;                      shift; shift ;;
    -h | --help )               PRINT_HELP=true;                    shift ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${TAGS}" ] && [ "${AUTO_TAG}" = "false" ]; then
  echo "Cannot push image without tag." >&2 ; exit 1 ;
fi

if [ ! -z "${DEST_LOGIN}" ] && [ "$ORACLE" = "false" ]; then
  docker login -u "${DEST_LOGIN}" -p "${DEST_PASS}" "${REGISTRY}" || { echo "Cannot login to docker image registry" ; exit 1 ; }
fi

echo "Creating plugin directory ${PLUGIN_DIR}"
mkdir -p "${PLUGIN_DIR}"

# Get debezium project version from pom.xml
project_version=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)

# Copy build debezium connectors and extra libs to plugins directory
pushd "${PLUGIN_DIR}" || exit
cp "$MAVEN_REPO"/io/debezium/debezium-connector-*/"${project_version}"/debezium-connector-*.zip .
unzip debezium-connector-\*.zip && rm debezium-connector-*.zip

cp "$MAVEN_REPO"/com/ibm/db2/jcc/*/jcc-*.jar debezium-connector-db2/
cp "$MAVEN_REPO"/com/oracle/database/jdbc/ojdbc8/*/ojdbc8-*.jar debezium-connector-oracle/

cp "$MAVEN_REPO"/io/apicurio/apicurio-registry-distro-connect-converter/*/apicurio-registry-*.zip .
cp "$MAVEN_REPO"/io/debezium/debezium-scripting/"${project_version}"/debezium-scripting-*.zip .

CONNECTORS='debezium-connector-oracle debezium-connector-db2 debezium-connector-sqlserver'\
' debezium-connector-mongodb debezium-connector-postgres debezium-connector-mysql'

# Copy groovy plugins, Apicurio and debezium scripting to each connector
for connector in $CONNECTORS
do
	unzip -o apicurio-registry-*.zip -d "${connector}/"
	cp "$MAVEN_REPO"/org/codehaus/groovy/groovy/*/groovy-*.jar "${connector}/"
  cp "$MAVEN_REPO"/org/codehaus/groovy/groovy-json/*/groovy-json-*.jar "${connector}/"
  cp "$MAVEN_REPO"/org/codehaus/groovy/groovy-jsr223/*/groovy-jsr223-*.jar "${connector}/"
  unzip -o debezium-scripting-*.zip -d "${connector}/"
done

rm apicurio-registry-*.zip
rm debezium-scripting-*.zip

prefix=""
if [ "${ORACLE}" = "false" ] ; then
  rm -rf debezium-connector-oracle
else
  echo "Changing quay organisation to private rh_integration since ORACLE connector is included"
  ORGANISATION="rh_integration"
  prefix="dbz-"
fi

popd || exit

echo "Copying Dockerfile to" "${BUILD_DIR}"
cp "$DOCKER_FILE" "${BUILD_DIR}/Dockerfile"

image=${REGISTRY}/${ORGANISATION}/${prefix}strimzi-kafka-upstream
target=${image}:strz-${CONNECT_BASE_TAG}-kafka-${KAFKA_VERSION}-apc-${APICURIO_VERSION}-dbz-${project_version}

pushd "${BUILD_DIR}" || exit
echo "[Build] Building $target"
docker build . -t "$target" --build-arg FROM_IMAGE=quay.io/strimzi/kafka:"${CONNECT_BASE_TAG}"-kafka-"${KAFKA_VERSION}"
popd || exit

if [ "${AUTO_TAG}" = "true" ] ; then
  echo "[Build] Pushing image ${target}"
  docker push ${target}
  [[ -z "${IMAGE_OUTPUT_FILE}" ]] || echo $target >> ${IMAGE_OUTPUT_FILE}
fi
for tag in ${TAGS}; do
  new_target="${image}:${tag}"
  echo "[Build] Pushing image ${new_target}"
  docker tag "${target}" "${new_target}"
  docker push "${new_target}"
  [[ -z "${IMAGE_OUTPUT_FILE}" ]] || echo "$new_target" >> ${IMAGE_OUTPUT_FILE}
done



