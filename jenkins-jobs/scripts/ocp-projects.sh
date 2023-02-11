#! /usr/bin/env bash
PROJECT="debezium"
COMPONENTS="mysql postgresql sqlserver mongo db2 oracle registry"
PROJECT_DELETE_RESOURCES="KafkaConnect Service"
SCC_ANYUID="mongo sqlserver db2 oracle"
SCC_PRIVILEGED="db2"
CREATE=false
DELETE=false

OPTS=$(getopt -o p:t: --long project:,tag:,create,delete,envfile:,components:,privileged:,skiproot,testsuite -n 'parse-options' -- "$@")
eval set -- "$OPTS"

while true; do
  case "$1" in
    -p | --project )    PROJECT=$2;                   shift; shift ;;
    -t | --tag )        PROJECT="${PROJECT}-$2";      shift; shift ;;
    -e | --envfile)     ENV_FILE=$2;                  shift; shift ;;
    --components )      COMPONENTS=$2;                shift;;
    --anyuid )          SCC_ANYUID=$2;                shift;;
    --privileged )      SCC_PRIVILEGED=$2;            shift;;
    --create )          CREATE=true;                  shift;;
    --delete )          DELETE=true;                  shift;;
    --skiproot)         SKIP_ROOT=true;               shift;;
    --testsuite)        TESTSUITE=true;          shift;;
    -h | --help )       PRINT_HELP=true;              shift;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

ENV_FILE=${ENV_FILE:-"${PROJECT}.ocp.env"}
# reset envar file
rm -f "${ENV_FILE}"

if ${CREATE} && ${DELETE}; then
  echo "Only one of --create or --delete permitted" >&2
  return
fi

function filevar() {
  echo "$1=$2" >> "${ENV_FILE}"
}

function delete_project() {
  for resource in ${PROJECT_DELETE_RESOURCES}; do
    oc delete "$resource" --all -n "${project}"
  done
  oc delete project "${project}"
}

#
# For given component executes
# On create
# - create project
# - set security contexts
# - add variable to envar file
#
# On delete
# - delete all KafkaConnect resources
# - delete all Service resources
# - delete the project
function process_component() {
    component=$1
    project=$2
    variable=$3



    if ${CREATE}; then
      filevar "${variable}" "${project}"
      oc new-project "${project}"
      [[ ${SCC_ANYUID[*]} =~ $component ]]      && oc adm policy add-scc-to-user anyuid      "system:serviceaccount:${project}:default"
      [[ ${SCC_PRIVILEGED[*]} =~ $component ]]  && oc adm policy add-scc-to-user privileged  "system:serviceaccount:${project}:default"
    fi

    if ${DELETE}; then
      delete_project "$project"
    fi
}


# Process ROOT project
if [ -z "${SKIP_ROOT}" ]; then
  process_component  "debezium" "${PROJECT}" "OCP_PROJECT_DEBEZIUM"
fi

# Process TESTSUITE project
if [ -n "${TESTSUITE}" ]; then
  process_component  "debezium" "${PROJECT}-testsuite" "OCP_PROJECT_DEBEZIUM_TESTSUITE"
fi

# Process component projects
for component in ${COMPONENTS}; do
  variable=$(echo "OCP_PROJECT_${component}" | awk '{print toupper($0)}')
  project=$(echo "${PROJECT}-${component}" | awk '{print tolower($0)}')

  process_component "${component}" "${project}" "${variable}"
done

if ${CREATE}; then
  echo "Environment generated"
  echo "==="
  cat "${ENV_FILE}"
  echo ""
  echo -e "Run \e[32msource ${ENV_FILE}\e[0m to apply"
fi
