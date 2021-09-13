#! /usr/bin/env bash

OPTS=$(getopt -o d:o: --long dir:,output: -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

OUTPUT="$(pwd)/artifacts.txt"
while true; do
  case "$1" in
    -d | --dir )                DIR=$2;          shift; shift ;;
    -o | --output )             OUTPUT=$2;                      shift; shift ;;
    -h | --help )               PRINT_HELP=true;                shift ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

shopt -s globstar nullglob
pushd "$DIR" || exit
rm -f "$OUTPUT"
for file in **/*.{zip,jar}; do
    prefix=$(echo "$file" | sed -rn 's@^(.*)-([[:digit:]].*([[:digit:]]|Final|SNAPSHOT))(.*)(\..*)$@\1@p')
    suffix=$(echo "$file" | sed -rn 's@^(.*)-([[:digit:]].*([[:digit:]]|Final|SNAPSHOT))(.*)(\..*)$@\4@p')
    artifact="$prefix$suffix"
    echo "$artifact"
    echo "$artifact:$file" >> "$OUTPUT"
done
popd