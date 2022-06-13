#!/bin/bash

clone_strimzi()
{
    local STRIMZI_REPO="https://github.com/strimzi/strimzi-kafka-operator.git" ;
    local PRODUCT_BUILD=false ;
    local STRIMZI_DOWNSTREAM_URL="" ;
    local STRIMZI_BRANCH="main" ;

    OPTS=$(getopt -o --long strimzi-repository:,strimzi-branch:,product-build,strimzi-downstream:,product-build:  -n 'clone-repositories' -- "$@")
    if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
    eval set -- "$OPTS"

    while true; do
        case "$1" in
            --strimzi-repository ) STRIMZI_REPO=$2;                shift 2;;
            --product-build )      PRODUCT_BUILD=$2;               shift 2;;
            --strimzi-downstream ) STRIMZI_DOWNSTREAM_URL=$2;      shift 2;;
            --strimzi-branch )     STRIMZI_BRANCH=$2;              shift 2;;
            -- ) shift; break ;;
            * ) break ;;
        esac
    done

    if [ "${PRODUCT_BUILD}" = false ] ;
    then
      # TODO why clone AND checkout?
        git clone --branch "${STRIMZI_BRANCH}" "${STRIMZI_REPO}" strimzi && git -C strimzi checkout "${STRIMZI_BRANCH}" || exit 2 ;
    elif [ -z "${STRIMZI_DOWNSTREAM_URL}" ] ;
    then 
        exit 2 ;
    else 
        curl --retry 7 -Lo /tmp/strimzi_downstream.zip "${STRIMZI_DOWNSTREAM_URL}" ; 
        unzip /tmp/strimzi_downstream.zip -d strimzi ;
        rm /tmp/strimzi_downstream.zip ;
    fi ;
}

# TODO downstream apicurio?
clone_apicurio()
{
  local APICURIO_REPO="https://github.com/Apicurio/apicurio-registry-operator.git" ;
  local APICURIO_BRANCH="master" ;

  # TODO why clone AND checkout?
  git clone --branch "${APICURIO_BRANCH}" "${APICURIO_REPO}" apicurio && git -C apicurio checkout "${APICURIO_BRANCH}" || exit 2 ;
}
