#!/usr/bin/env bash

OPTS=`getopt -o c:e:r:a: --long connector:,env-file:,results-folder:,attributes: -n 'parse-options' -- "$@"`
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

while true; do
  case "$1" in
    -c | --connector)           CONNECTOR=$2;                   shift; shift ;;
    -e | --env-file )           ENV_FILE=$2;                    shift; shift ;;
    -r | --results-folder )     RESULTS=$2;                     shift; shift ;;
    -a | --attributes )         ATTRIBUTES=$2;                  shift; shift ;;
    -h | --help )               PRINT_HELP=true;                shift ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

echo "Reporting to report portal!"

ENV_FILE="$PWD/$ENV_FILE"
RESULTS="$PWD/$RESULTS"
touch "$ENV_FILE"

if [ "$CONNECTOR" == "true" ] ; then
  echo "PROJECT_NAME=debezium-connectors" >> "$ENV_FILE"
  echo "LAUNCH_NAME=connector-test-run" >> "$ENV_FILE"
  echo "DESCRIPTION=$(echo "Matrix job for db2 connector executed in $BUILD_URL")" >> "$ENV_FILE"
else
  echo "PROJECT_NAME=debezium-systemtests" >> "$ENV_FILE"
  echo "LAUNCH_NAME=system-tests" >> "$ENV_FILE"
  echo "DESCRIPTION=$(echo "Systemtests executed in $BUILD_URL")" >> "$ENV_FILE"

  if [ "$PRODUCT_BUILD" == "true" ] ; then\
    docker run -it --entrypoint sh "$IMAGE_DBZ_AS" -c "cat /opt/plugins/artifacts.txt" > artifacts.txt
    REGISTRY_VERSION=$(grep -Po "(?<=registry-)(([0-9]+\.)?([0-9]+\.)?([0-9]+)\.[^-]*)" artifacts.txt )
    DEBEZIUM_VERSION=$(grep -Po "(?<=debezium-)(([0-9]+\.)?([0-9]+\.)?([0-9]+)\.[^-]*)" artifacts.txt -m 1)
    rm -rf artifacts.txt
    STREAMS_VERSION=$(echo $IMAGE_CONNECT_RHEL |  grep -Po "(?<=amq-)(([0-9]+\.)?([0-9]+\.)?([0-9]+)\.[^-]*)")
    ATTRIBUTES=$ATTRIBUTES" Registry-version:$REGISTRY_VERSION Streams-version:$STREAMS_VERSION Debezium-version:$DEBEZIUM_VERSION"
  fi
fi

FINAL_ATTRIBUTES='ATTRIBUTES=['
for attr in $ATTRIBUTES; do
  FINAL_ATTRIBUTES=$FINAL_ATTRIBUTES'"'$attr'", '
done
FINAL_ATTRIBUTES=${FINAL_ATTRIBUTES::-2}
FINAL_ATTRIBUTES=$FINAL_ATTRIBUTES']'

echo "$FINAL_ATTRIBUTES" >> "$ENV_FILE"

docker run -v "$RESULTS":/report_data --env-file "$ENV_FILE" -e RP_TOKEN="$RP_TOKEN" quay.io/rh_integration/debezium-reporting-tools:latest
rm -rf "$RESULTS"
rm -rf "$ENV_FILE"

