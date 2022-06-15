#!/bin/bash

clone_component()
{
    OPTS=$(getopt -o c: --long component:,git-repository:,git-branch:,downstream-url:,product-build:  -n 'clone-repositories' -- "$@")
    if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
    eval set -- "$OPTS"

    while true; do
        case "$1" in
            -c | --component )  COMPONENT=$2;          shift 2;;
            --git-repository )  GIT_REPO=$2;           shift 2;;
            --git-branch )      GIT_BRANCH=$2;         shift 2;;
            --product-build )   PRODUCT_BUILD=$2;      shift 2;;
            --downstream-url )  DOWNSTREAM_URL=$2;     shift 2;;
            -- ) shift; break ;;
            * ) break ;;
        esac
    done

    if [ "${COMPONENT}" != "apicurio" ] && [ "${COMPONENT}" != "strimzi" ] ;
    then
      exit 2 ;
    fi ;

    if [ "${PRODUCT_BUILD}" = false ] ;
    then
        git clone --branch "${GIT_BRANCH}" "${GIT_REPO}" "${COMPONENT}" || exit 2 ;
    elif [ -z "${DOWNSTREAM_URL}" ] ;
    then
        exit 2 ;
    else
        curl --retry 7 -Lo /tmp/"${COMPONENT}".zip "${DOWNSTREAM_URL}" ;
        unzip /tmp/"${COMPONENT}".zip -d "${COMPONENT}" ;
        rm /tmp/"${COMPONENT}".zip ;
    fi ;
}
