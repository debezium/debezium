#!/bin/bash

create_projects()
{
    for project in "$@"
    do 
        oc new-project "${project}"
    done    
}

delete_projects()
{
    for project in "$@"
    do 
        oc delete project "${project}"
    done
}

clone_repositories()
{
    local DEBEZIUM_REPO="https://github.com/debezium/debezium.git" ;
    local STRIMZI_REPO="https://github.com/strimzi/strimzi-kafka-operator.git" ;
    local PRODUCT_BUILD=false ;
    local STRIMZI_DOWNSTREAM_URL="" ;
    local DBZ_BRANCH="main" ;
    local STRIMZI_BRANCH="main" ;

    OPTS=`getopt -o --long dbz-repository:,dbz-branch:,strimzi-repository:,strimzi-branch:,product-build,strimzi-downstream:,product-build:  -n 'clone-repositories' -- "$@"`
    if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
    eval set -- "$OPTS"

    while true; do
        case "$1" in
            --dbz-repository )     DEBEZIUM_REPO=$2;               shift 2;;
            --strimzi-repository ) STRIMZI_REPO=$2;                shift 2;;
            --product-build )      PRODUCT_BUILD=$2;               shift 2;;
            --strimzi-downstream ) STRIMZI_DOWNSTREAM_URL=$2;      shift 2;;
            --dbz-branch )         DBZ_BRANCH=$2;                  shift 2;;
            --strimzi-branch )     STRIMZI_BRANCH=$2;              shift 2;;
            -- ) shift; break ;;
            * ) break ;;
        esac
    done

    git clone ${DEBEZIUM_REPO} debezium && git -C debezium checkout ${DBZ_BRANCH} || exit 1 ;
    if [ "${PRODUCT_BUILD}" = false ] ; 
    then 
        git clone ${STRIMZI_REPO} strimzi && git -C strimzi checkout ${STRIMZI_BRANCH} || exit 2 ;
    elif [ -z "${STRIMZI_DOWNSTREAM_URL}" ] ;
    then 
        exit 2 ;
    else 
        curl --retry 7 -Lo /tmp/strimzi_downstream.zip "${STRIMZI_DOWNSTREAM_URL}" ; 
        unzip /tmp/strimzi_downstream.zip -d strimzi ;
        rm /tmp/strimzi_downstream.zip ;
    fi ;
}   
