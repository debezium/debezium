#! /usr/bin/env bash
set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
COPY_IMAGES=true
REGISTRY="quay.io"

OPTS=`getopt -o d:i:r:o:f:s --long dir:,images:,registry:,organisation:,deployment-desc:,skip-copy,dest-login:,dest-pass:,img-output: -n 'parse-options' -- "$@"`
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

while true; do
  case "$1" in
    -d | --dir )                INSTALL_SOURCE_DIR=$2;          shift; shift ;;
    -i | --images )             IMAGES=$2;                      shift; shift ;;
    -r | --registry )           REGISTRY=$2;                    shift; shift ;;
    -o | --organisation )       ORGANISATION=$2;                shift; shift ;;
    -f | --deployment-desc )    DEPLOYMENT_DESC=$2;             shift; shift ;;
    -s | --skip-copy )          COPY_IMAGES=false;              shift ;;
    --dest-login )              DEST_LOGIN=$2;                  shift; shift ;;
    --dest-pass )               DEST_PASS=$2;                   shift; shift ;;
    --img-output )              IMAGE_OUTPUT_FILE=$2;           shift; shift ;;
    -h | --help )               PRINT_HELP=true;                shift ;;
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

    prefix=`echo $source | sed -rn 's|.*/[^/]+/([^-]*)-(.*):(.*)$|\1|p'`
    name=`echo $source | sed -rn 's|.*/[^/]+/([^-]*)-(.*):(.*)$|\2|p'`
    tag=`echo $source | sed -rn 's|.*/[^/]+/([^-]*)-(.*):(.*)$|\3|p'`
    target=${registry}/${organisation}/${prefix}-${name}:${tag}


    if [ "$COPY_IMAGES" = true ] ; then
        echo "[Image Copy] Pushing image $source as $target"
        docker pull "$source"
        docker tag "$source" "$target"
        docker push "$target"
    fi

    # Replace images
    echo "[Deployment Transformation] replacing image ${target}"
    sed -i "s@registry.redhat.io/.*/${name}:.*\$@${target}@" "${INSTALL_SOURCE_DIR}/${DEPLOYMENT_DESC}"
    [[ -z "${IMAGE_OUTPUT_FILE}" ]] || echo $target >> ${IMAGE_OUTPUT_FILE}
}

for image in $IMAGES; do
    echo "[Processing] $image"
    process_image $image $REGISTRY $ORGANISATION
done
