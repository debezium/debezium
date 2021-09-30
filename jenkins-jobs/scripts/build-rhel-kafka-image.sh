#! /usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DOCKER_FILE=${DIR}/../docker/rhel_kafka/Dockerfile
PLUGIN_DIR="plugins"
EXTRA_LIBS=""

OPTS=$(getopt -o d:i:a:k:l:f:r:o:t: --long dir:,image:,archive-urls:,kafka-url:,libs:,dockerfile:,registry:,organisation:,tag:,dest-login:,dest-pass:,img-output: -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

while true; do
  case "$1" in
    -d | --dir )                BUILD_DIR=$2;
                                PLUGIN_DIR=${BUILD_DIR}/plugins;    shift; shift ;;
    -i | --image )              IMAGE=$2;                           shift; shift ;;
    -a | --archive-urls )       ARCHIVE_URLS=$2;                    shift; shift ;;
    -k | --kafka-url )          KAFKA_URL=$2;                       shift; shift ;;
    -l | --libs )               EXTRA_LIBS=$2;                      shift; shift ;;
    -f | --dockerfile )         DOCKER_FILE=$2;                     shift; shift ;;
    -r | --registry )           REGISTRY=$2;                        shift; shift ;;
    -o | --organisation )       ORGANISATION=$2;                    shift; shift ;;
    -t | --tag )                TAG=$2;                             shift; shift ;;
    --dest-login )              DEST_LOGIN=$2;                      shift; shift ;;
    --dest-pass )               DEST_PASS=$2;                       shift; shift ;;
    --img-output )              IMAGE_OUTPUT_FILE=$2;               shift; shift ;;
    -h | --help )               PRINT_HELP=true;                    shift ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ ! -z "${DEST_LOGIN}" ] ; then
  docker login -u "${DEST_LOGIN}" -p "${DEST_PASS}" "${REGISTRY}"
fi

echo "Creating plugin directory ${PLUGIN_DIR}"
mkdir -p "${PLUGIN_DIR}"

pushd "${PLUGIN_DIR}" || exit
for archive in ${ARCHIVE_URLS}; do
    echo "[Processing] ${archive}"
    curl -OJs "${archive}" && unzip \*.zip && rm *.zip
    connectors_version=$(echo "$archive" | sed -rn 's|.*AMQ-CDC-(.*)/.*$|\1|p')
done

for input in ${EXTRA_LIBS}; do
    echo "[Processing] ${input} "
    lib=$(echo ${input} | awk -F "::"  '{print $1}' | xargs)
    dest=$(echo ${input} |  awk -F "::"  '{print $2}' | xargs)

    curl -OJs "${lib}"
    if [[ "${lib}" =~ ^.*\.zip$ ]] ; then
        unzip -od ${dest} \*.zip && rm *.zip
    else
        mv *.jar "${dest}"
    fi
done
popd || exit

echo "Copying Docker_entrypoint.sh and scripts to" "${BUILD_DIR}"
cp "${DIR}"/../docker/rhel_kafka/* "$BUILD_DIR"

echo "Copying Dockerfile to" "${BUILD_DIR}"
cp "$DOCKER_FILE" "$BUILD_DIR"

if [ -z "$TAG" ] ; then
  amq_version=$(echo "${KAFKA_URL}" | sed -rn 's|.*AMQ-STREAMS-(.*)/.*$|\1|p')
  image_dbz=debezium-testing-rhel8:amq-${amq_version}-dbz-${connectors_version}
  target=${REGISTRY}/${ORGANISATION}/${image_dbz}
else
  target=$TAG
fi

pushd "${BUILD_DIR}" || exit
#change absolute path of PLUGIN_DIR to relative so docker doesnt crash
PLUGIN_DIR_BUILDARG="./${PLUGIN_DIR##*/}"

echo "[Build] Building ${image_dbz} from ${IMAGE}"
docker build . -t "$target" --build-arg IMAGE="${IMAGE}" --build-arg KAFKA_SOURCE_PATH="${KAFKA_URL}" --build-arg DEBEZIUM_CONNECTORS="${PLUGIN_DIR_BUILDARG}"
popd || exit

echo "[Build] Pushing image ${target}"
docker push "$target"

[[ -z "${IMAGE_OUTPUT_FILE}" ]] || echo "$target" >> "${IMAGE_OUTPUT_FILE}"
