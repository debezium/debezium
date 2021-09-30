#! /usr/bin/env bash
set -x
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DOCKER_FILE=${DIR}/../docker/artifact-server/Dockerfile
PLUGIN_DIR="plugins"
EXTRA_LIBS=""


OPTS=$(getopt -o d:a:l:f:r:o:t:a: --long dir:,archive-urls:,libs:,dockerfile:,registry:,organisation:,tags:,auto-tag:,dest-login:,dest-pass:,img-output: -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

while true; do
  case "$1" in
    -d | --dir )                BUILD_DIR=$2;
                                PLUGIN_DIR=${BUILD_DIR}/plugins;    shift; shift ;;
    -a | --archive-urls )       ARCHIVE_URLS=$2;                    shift; shift ;;
    -l | --libs )               EXTRA_LIBS=$2;                      shift; shift ;;
    -f | --dockerfile )         DOCKER_FILE=$2;                     shift; shift ;;
    -r | --registry )           REGISTRY=$2;                        shift; shift ;;
    -o | --organisation )       ORGANISATION=$2;                    shift; shift ;;
    -t | --tags )               TAGS=$2;                            shift; shift ;;
    -a | --auto-tag )           AUTO_TAG=$2;                         shift; shift ;;
    --dest-login )              DEST_LOGIN=$2;                      shift; shift ;;
    --dest-pass )               DEST_PASS=$2;                       shift; shift ;;
    --img-output )              IMAGE_OUTPUT_FILE=$2;               shift; shift ;;
    -h | --help )               PRINT_HELP=true;                    shift ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${TAGS}" ] && [ "${AUTO_TAG}" = false ]; then
  echo "Cannot push image without tag." >&2 ; exit 1 ;
fi

if [ ! -z "${DEST_LOGIN}" ] ; then
  docker login -u "${DEST_LOGIN}" -p "${DEST_PASS}" "${REGISTRY}"
fi

echo "Creating plugin directory ${PLUGIN_DIR}"
mkdir -p "${PLUGIN_DIR}"

pushd "${PLUGIN_DIR}" || exit
for archive in ${ARCHIVE_URLS}; do
    echo "[Processing] ${archive}"
    lib=$(echo ${archive} | awk -F "::"  '{print $1}' | xargs)
    dest=$(echo ${archive} |  awk -F "::"  '{print $2}' | xargs)

    if [ -z "$dest" ] ; then
      curl -OJs "${lib}"
    else
      mkdir -p "${dest}" && pushd ${dest} && curl -OJs "${lib}" && popd || exit
    fi
    connectors_version=$(echo "$archive" | sed -rn 's|.*AMQ-CDC-(.*)/.*$|dbz-\1|p')
done

for input in ${EXTRA_LIBS}; do
    echo "[Processing] ${input} "
    lib=$(echo ${input} | awk -F "::"  '{print $1}' | xargs)
    dest=$(echo ${input} |  awk -F "::"  '{print $2}' | xargs)

    if [ -z "$dest" ] ; then
      curl -OJs "${lib}"
    else
      mkdir -p "${dest}" && pushd ${dest} && curl -OJs "${lib}" && popd || exit
    fi

done
popd || exit

echo "Copying scripts to" "${BUILD_DIR}"
cp "${DIR}"/../docker/artifact-server/* "$BUILD_DIR"

echo "Copying Dockerfile to" "${BUILD_DIR}"
cp "$DOCKER_FILE" "$BUILD_DIR"

image_dbz=dbz-artifact-server
target=${REGISTRY}/${ORGANISATION}/${image_dbz}:${connectors_version}

pushd "${BUILD_DIR}" || exit
echo "[Build] Building $target"
docker build . -t "$target"
popd || exit

if [ "${AUTO_TAG}" ] ; then
  echo "[Build] Pushing image ${target}"
  docker push ${target}
  [[ -z "${IMAGE_OUTPUT_FILE}" ]] || echo $target >> ${IMAGE_OUTPUT_FILE}
fi
for tag in ${TAGS}; do
  new_target="${REGISTRY}/${ORGANISATION}/${image_dbz}:${tag}"
  echo "[Build] Pushing image ${new_target}"
  docker tag "${target}" "${new_target}"
  docker push "${new_target}"
  [[ -z "${IMAGE_OUTPUT_FILE}" ]] || echo "$new_target" >> ${IMAGE_OUTPUT_FILE}
done



