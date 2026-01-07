#! /usr/bin/env bash

CONNECTORS="db2 mongodb mysql mariadb oracle postgres sqlserver jdbc"
OPTS=$(getopt -o d:o:c: --long dir:,output:,connectors: -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

OUTPUT="$(pwd)/artifacts.txt"
while true; do
  case "$1" in
    -d | --dir )                DIR=$2;                         shift; shift ;;
    -o | --output )             OUTPUT=$2;                      shift; shift ;;
    -c | --connectors )         CONNECTORS=$2;                  shift; shift ;;
    -h | --help )               PRINT_HELP=true;                shift ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

shopt -s globstar
pushd "$DIR" || exit
rm -f "$OUTPUT"
for connector in ${CONNECTORS}; do
    file=""
    if [[ $(ls **/*"${connector}"*.zip) ]]; then
        file=$(ls **/*"${connector}"*.zip)
    fi
    artifact="debezium-connector-$connector"
    echo "$artifact"
    echo "$artifact::$file" >> "$OUTPUT"
done


scripting=$(ls **/*scripting*.{zip,jar})
artifact="debezium-scripting"
echo "$artifact"
echo "$artifact::$scripting" >> "$OUTPUT"

converter=$(ls **/*converter*.{zip,jar})
artifact="connect-converter"
echo "$artifact"
echo "$artifact::$converter" >> "$OUTPUT"

for driver in **/jdbc/*.{zip,jar}; do
    name=$(echo "$driver" | sed -rn 's@^(.*)-([[:digit:]].*([[:digit:]]|Final|SNAPSHOT))(.*)(\..*)$@\1@p')
    artifact="$name"
        if [[ ! $artifact ]]; then
                continue
        fi
    echo "$artifact"
    echo "$artifact::$driver" >> "$OUTPUT"
done

for groovy_script in **/groovy/*.{zip,jar}; do
    name=$(echo "$groovy_script" | sed -rn 's@^(.*)-[0-9]\..*$@\1@p')
    artifact="$name"
    if [[ ! $artifact ]]; then
        continue
    fi
    echo "$artifact"
    echo "$artifact::$groovy_script" >> "$OUTPUT"
done

for jackson_lib in **/jackson/*.jar; do
    name=$(echo "$jackson_lib" | sed -rn 's@^(.*)-[0-9]\..*$@\1@p')
    artifact="$name"
    if [[ ! $artifact ]]; then
        continue
    fi
    echo "$artifact"
    echo "$artifact::$jackson_lib" >> "$OUTPUT"
done

popd || exit
