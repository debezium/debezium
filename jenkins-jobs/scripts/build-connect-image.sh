#! /usr/bin/env bash
set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
COPY_IMAGES=true
REGISTRY="quay.io"
DOCKER_FILE=${DIR}/../docker/Dockerfile.Strimzi
PLUGIN_DIR="plugins"
EXTRA_LIBS=""

OPTS=`getopt -o d:i:a:l:f:r:o: --long dir:,images:,archive-urls:,libs:,dockerfile:,registry:,organisation:,dest-login:,dest-pass:,img-output: -n 'parse-options' -- "$@"`
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

while true; do
  case "$1" in
    -d | --dir )                BUILD_DIR=$2;
                                PLUGIN_DIR=${BUILD_DIR}/plugins;    shift; shift ;;
    -i | --images )             IMAGES=$2;                          shift; shift ;;
    -a | --archive-urls )       ARCHIVE_URLS=$2;                    shift; shift ;;
    -l | --libs )               EXTRA_LIBS=$2;                      shift; shift ;;
    -f | --dockerfile )         DOCKER_FILE=$2;                     shift; shift ;;
    -r | --registry )           REGISTRY=$2;                        shift; shift ;;
    -o | --organisation )       ORGANISATION=$2;                    shift; shift ;;
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

function process_image() {
    source=$1
    registry=$2
    organisation=$3

    prefix_dbz="dbz"
    name=`echo $source | sed -rn 's/.*\/[^\/]+\/([^-]*)-(.*):(.*)$/\2/p'`
    tag=`echo $source | sed -rn 's/.*\/[^\/]+\/([^-]*)-(.*):(.*)$/\3/p'`

    image_dbz=${prefix_dbz}-${name}:${tag}
    target=${registry}/${organisation}/${image_dbz}

    if [[ "$name" =~ ^amq-streams-kafka-.*$ ]] ; then
        echo "[Build] Building ${image_dbz} from ${source}"

        pushd "${BUILD_DIR}" || exit
        docker build -f "${DOCKER_FILE}" . -t "${target}" --build-arg FROM_IMAGE="${source}"
        popd || exit

        echo "[Build] Pushing image ${target}"
        docker push "${target}"
        [[ -z "${IMAGE_OUTPUT_FILE}" ]] || echo "$target" >> "${IMAGE_OUTPUT_FILE}"
    else
        echo "[Build] ${source} not applicable for build"
    fi

}

echo "Creating plugin directory ${PLUGIN_DIR}"
mkdir -p "${PLUGIN_DIR}"

pushd "${PLUGIN_DIR}" || exit
for archive in ${ARCHIVE_URLS}; do
    echo "[Processing] ${archive}"
    curl -OJs ${archive} && unzip \*.zip && rm *.zip
done

connector_dirs=$(ls)

for input in ${EXTRA_LIBS}; do
    echo "[Processing] ${input}"
    lib=`echo ${input} | awk -F "::"  '{print $1}' | xargs`
    dest=`echo ${input} |  awk -F "::"  '{print $2}' | xargs`

    curl -OJs "${lib}"
    if [[ "${dest}" == '*' ]] ; then
        if [[ "${lib}" =~ ^.*\.zip$ ]] ; then
            echo $connector_dirs | xargs -n 1 unzip -o \*.zip -d
            rm *.zip
        else
            echo $connector_dirs | xargs -n 1 cp *.jar
            rm *.jar
        fi
        continue;
    fi
    if [[ "${lib}" =~ ^.*\.zip$ ]] ; then
        unzip -od "${dest}" \*.zip && rm *.zip
    else
        mv *.jar "${dest}"
    fi
done
popd || exit

for image in $IMAGES; do
    echo "[Processing] $image"
    process_image "${image}" "${REGISTRY}" "${ORGANISATION}"
done