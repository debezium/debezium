#! /usr/bin/env bash
PRINT_HELP=false
TMP_FOLDER_NAME="artifact_tmp"
EXPECTED_COMPONENTS="debezium-connector-db2 debezium-connector-mongodb debezium-connector-mysql debezium-connector-oracle debezium-connector-postgres debezium-connector-sqlserver debezium-scripting"

print_usage() {
  echo "Usage: maven-artifact-check [ -u | --url ] <ARTIFACT URL> [ -f | --file ] <artifact archive file> [ --components ] <expected components list>]"
  echo "downloads the maven artifact archive from URL or uses given file, unzips it and validates its artifact_version, components matching EXPECTED_COMPONENTS and checksums"
  echo "--components needs to be the last argument in order to parse an array of arguments"
}

OPTS=$(getopt -o u:f:h --long url:,components:,file:,help -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

while true; do
  case "$1" in
    -f | --file )           ARTIFACT_FILE=$2;           shift; shift ;;
    -u | --url )            ARTIFACT_URL=$2;            shift; shift ;;
    --components )          EXPECTED_COMPONENTS=$2      shift; shift ;;
    -h | --help )           PRINT_HELP=true;            shift ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ ${PRINT_HELP} == true ]; then
  print_usage
  exit
fi

if [ -z "${ARTIFACT_URL}" ] && [ -z "${ARTIFACT_FILE}" ]; then
  echo "either artifact file path or url must be specified!"
  echo ""
  print_usage
  exit 1
fi

return_code=0

# check url/filename, parse artifact version
if [ -n "${ARTIFACT_URL}" ]; then
    URL_PATTERN='^http[s]?:\/\/.*\/(debezium-([0-9]\.[0-9]\.[0-9]).*\.zip)$'
    [[ ${ARTIFACT_URL} =~ ${URL_PATTERN} ]]
    zip_name="${BASH_REMATCH[1]}"
    artifact_version="${BASH_REMATCH[2]}"
    if [ -z "${artifact_version}" ]; then
        echo "invalid artifact url"
        return_code=1
    fi
    echo "${zip_name}"
    curl "${ARTIFACT_URL}" --output "${zip_name}"
else
    FILE_PATTERN='^.*(debezium-([0-9]\.[0-9]\.[0-9]).*\.zip)$'
    [[ ${ARTIFACT_FILE} =~ ${FILE_PATTERN} ]]
        zip_name="${BASH_REMATCH[1]}"
        artifact_version="${BASH_REMATCH[2]}"
    if [ -z "${artifact_version}" ]; then
        echo "invalid artifact file"
        return_code=1
    fi
fi

# unzip
unzip "${zip_name}" -d ${TMP_FOLDER_NAME}
pushd ${TMP_FOLDER_NAME} || exit 1

# enter the artifact directory and check directory structure
for file in *; do
    if [ -d "${file}" ]; then
        # validate artifact_version from dir name
        if [[ ! "${file}" == *"${artifact_version}"* ]]; then
            echo "invalid artifact_version in root directory name"
            return_code=1
        fi
        pushd "${file}" || exit 1
    fi
done

if [ ! -d "./maven-repository/io/debezium/" ]; then
    echo "invalid directory structure of artifact!"
    exit 1
fi

pushd "maven-repository/io/debezium/" >> /dev/null || exit 1

# check if correct artifact components are present
actual_components=($(ls))
sorted_expected_components=$(printf '%s ' "${EXPECTED_COMPONENTS[@]}" | sort | xargs)
if [[ ! "${sorted_expected_components[*]}" == "${actual_components[*]}" ]]; then
    echo "components of artifact don't match expected components"
    echo "expected components: ${sorted_expected_components[*]}"
    echo "actual_components ${actual_components[*]}"
    exit 1
fi

# iterate components
for component in *; do
    if [ ! -d "${component}" ]; then
        echo "component ${component} is not a directory"
        return_code=1
        continue
    else
        echo "VALIDATING COMPONENT ${component}"
        pushd "${component}" >> /dev/null || exit 1
        dir_content=($(ls))

        # check that only one directory is present
        if [ ${#dir_content[@]} != 1 ]; then
            echo "component directory should only contain a single folder"
            return_code=1
        fi

        # validate artifact version of component directory
        if [[ ! "${dir_content[0]}" == *"${artifact_version}"* ]]; then
            echo "invalid artifact version in component ${component}"
            return_code=1
        fi
        pushd "${dir_content[0]}" >> /dev/null || exit 1

        # validate component directory contents
        version_pattern=$(echo "${artifact_version}" | sed "s/\./\\\./g")

        archive_pattern="^${component}-${version_pattern}.*\.zip$"
        md5_pattern="^${component}-${version_pattern}.*\.zip\.md5$"
        sha1_pattern="^${component}-${version_pattern}.*\.zip\.sha1$"

        archive_filename=""
        md5_filename=""
        sha1_filename=""

        for file in *; do
            [[ ${file} =~ ${archive_pattern} ]]

            if [ -n "${BASH_REMATCH[0]}" ]; then
                archive_filename=${file}
            fi

            [[ ${file} =~ ${md5_pattern} ]]
            if [ -n "${BASH_REMATCH[0]}" ]; then
                md5_filename=${file}
            fi

            [[ ${file} =~ ${sha1_pattern} ]]
            if [ -n "${BASH_REMATCH[0]}" ]; then
                sha1_filename=${file}
            fi
        done

        if [ -z "${archive_filename}" ]; then
            echo "archive of component ${component} missing!"
            return_code=1
        fi

        if [ -z "${md5_filename}" ]; then
            echo "md5 of component ${component} missing!"
            return_code=1
        fi

        if [ -z "${sha1_filename}" ]; then
            echo "sha1 of component ${component} missing!"
            return_code=1
        fi

        # check sha1 hash
        sha1_sum="$(cat "${sha1_filename}")"
        expected_sha1_sum=$(sha1sum "${archive_filename}" | head -n1 | awk '{print $1;}')
        if [ "${sha1_sum}" != "${expected_sha1_sum}" ]; then
            echo "sha1 checksum of component ${component} is incorrect!"
            return_code=1
        fi

        # check md5 hash
        md5_sum="$(cat "${md5_filename}")"
        expected_md5_sum=$(md5sum "${archive_filename}" | head -n1 | awk '{print $1;}')
        if [ "${md5_sum}" != "${expected_md5_sum}" ]; then
            echo "md5 checksum of component ${component} is incorrect!"
            return_code=1
        fi

        popd >> /dev/null || exit 1
        popd >> /dev/null || exit 1
    fi
done

exit ${return_code}
