#!/bin/bash

clone_component()
{
    OPTS=$(getopt -o c: --long component:,git-repository:,git-branch:,product-build:  -n 'clone-repositories' -- "$@")
    if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
    eval set -- "$OPTS"

    while true; do
        case "$1" in
            -c | --component )  COMPONENT=$2;          shift 2;;
            --git-repository )  GIT_REPO=$2;           shift 2;;
            --git-branch )      GIT_BRANCH=$2;         shift 2;;
            --product-build )   PRODUCT_BUILD=$2;      shift 2;;
            -- ) shift; break ;;
            * ) break ;;
        esac
    done

    if [ "${COMPONENT}" != "apicurio" ] && [ "${COMPONENT}" != "strimzi" ] ;
    then
      echo "unknown component: ${COMPONENT}"
      exit 2 ;
    fi ;

    if [ "${PRODUCT_BUILD}" == false ];
    then
        git clone --branch "${GIT_BRANCH}" "${GIT_REPO}" "${COMPONENT}" || exit 2 ;
    else
        unzip "${COMPONENT}"/*.zip -d "${COMPONENT}" ;
        rm "${COMPONENT}"/*.zip ;
    fi ;
}
