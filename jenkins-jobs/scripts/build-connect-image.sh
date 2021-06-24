#! /usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
COPY_IMAGES=true
REGISTRY="quay.io"
DOCKER_FILE=${DIR}/../docker/Dockerfile.AMQ
PLUGIN_DIR="plugins"
EXTRA_LIBS=""

OPTS=`getopt -o d:i:a:l:f:r:o: --long dir:,images:,archive-urls:,libs:,dockerfile:,registry:,organisation:,dest-creds:,src-creds:,img-output: -n 'parse-options' -- "$@"`
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
    --dest-creds )              DEST_CREDS="--dest-creds $2";       shift; shift ;;
    --src-creds )               SRC_CREDS="--src-creds $2";         shift; shift ;;
    --img-output )              IMAGE_OUTPUT_FILE=$2;               shift; shift ;;
    -h | --help )               PRINT_HELP=true;                    shift ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

function process_image() {
    source=$1
    registry=$2
    organisation=$3

    prefix_dbz="dbz"
    prefix=`echo $source | sed -rn 's/.*\/[^\/]+\/([^-]*)-(.*):(.*)$/\1/p'`
    name=`echo $source | sed -rn 's/.*\/[^\/]+\/([^-]*)-(.*):(.*)$/\2/p'`
    tag=`echo $source | sed -rn 's/.*\/[^\/]+\/([^-]*)-(.*):(.*)$/\3/p'`

    image=${prefix}-${name}:${tag}
    image_dbz=${prefix_dbz}-${name}:${tag}
    target=${registry}/${organisation}/${image_dbz}


    if [[ "$name" =~ ^amq-streams-kafka-.*$ ]] ; then
        echo "[Build] Building ${image_dbz} from ${image}"
        skopeo --override-os "linux" copy --src-tls-verify=false "docker://$source" "docker-daemon:${image}"
        cd ${BUILD_DIR}
        docker build -f ${DOCKER_FILE} . -t ${image_dbz} --build-arg FROM_IMAGE=${image}
        cd -
        echo "[Build] Pushing image ${target}"
        skopeo --override-os "linux" copy --src-tls-verify=false ${DEST_CREDS} "docker-daemon:${image_dbz}" "docker://$target"
        [[ -z "${IMAGE_OUTPUT_FILE}" ]] || echo $target >> ${IMAGE_OUTPUT_FILE}
    else
        echo "[Build] ${image} not applicable for build"
    fi

}

echo "Creating plugin directory ${PLUGIN_DIR}"
mkdir -p "${PLUGIN_DIR}"

cd ${PLUGIN_DIR}
for archive in ${ARCHIVE_URLS}; do
    echo "[Processing] ${archive}"
    curl -OJs ${archive} && unzip \*.zip && rm *.zip
done

for input in ${EXTRA_LIBS}; do
    echo "[Processing] input"
    lib=`echo ${input} | awk -F "::"  '{print $1}' | xargs`
    dest=`echo ${input} |  awk -F "::"  '{print $2}' | xargs`

    curl -OJs ${lib}
    if [[ "${lib}" =~ ^.*\.zip$ ]] ; then
        unzip -od ${dest} \*.zip && rm *.zip
    else
        mv *.jar ${dest}
    fi
done
cd -

for image in $IMAGES; do
    echo "[Processing] $image"
    process_image "${image}" "${REGISTRY}" "${ORGANISATION}"
done