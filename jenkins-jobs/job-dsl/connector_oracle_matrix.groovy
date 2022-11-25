matrixJob('connector-debezium-oracle-matrix-test') {
    displayName('Debezium Oracle Connector Test Matrix')
    description('Executes tests for Oracle connector with Oracle matrix')
    label('Slave')

    axes {
        text('ORACLE_VERSION', '21.3.0', '19.3.0', '19.3.0-noncdb', '12.2.0.1')
        label("Node", "NodeXL")
    }

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }


    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', 'main', 'A branch/tag from which Debezium is built')
//          QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', 'Quay.io credentials id')
//          PRODUCT CONFIG
        stringParam('SOURCE_URL', "", "URL to productised sources")
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')
    }

    triggers {
        cron('H 04 * * *')
    }

    wrappers {
        preBuildCleanup()

        timeout {
            noActivity(1200)
        }

        credentialsBinding{
            usernamePassword('QUAY_USERNAME', 'QUAY_PASSWORD', 'rh-integration-quay-creds')
            string('RP_TOKEN', 'report-portal-token')
        }
    }

    publishers {
        archiveArtifacts {
            pattern('**/archive.tar.gz')
        }
        archiveJunit('**/target/surefire-reports/*.xml')
        archiveJunit('**/target/failsafe-reports/*.xml')
        mailer('debezium-qe@redhat.com', false, true)
    }

    logRotator {
        daysToKeep(7)
        numToKeep(5)
    }

    steps {
        shell ('''

# Ensure WS cleaup
ls -A1 | xargs rm -rf
set -x

# Retrieve sources
if [ "$PRODUCT_BUILD" == true ] ; then
    export MAVEN_OPTS="-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true"
    PROFILE_PROD="-Ppnc"
    curl -OJs $SOURCE_URL && unzip debezium-*-src.zip
    pushd debezium-*-src
    pushd $(ls | grep -P 'debezium-[^-]+.Final')
    ATTRIBUTES="downstream Oracle $ORACLE_VERSION"
else
    git clone $REPOSITORY .
    git checkout $BRANCH
    ATTRIBUTES="upstream Oracle $ORACLE_VERSION"
fi

# Run database
docker login -u=${QUAY_USERNAME} -p=${QUAY_PASSWORD} quay.io
docker run --name oracledb -d -p 1521:1521  quay.io/rh_integration/dbz-oracle:${ORACLE_VERSION}
until docker inspect oracledb | grep '"Status": "healthy"'  2>/dev/null
do
    echo "waiting for database container to start"
    sleep 10
done

# Prepare tests
ORACLE_ARTIFACT_VERSION=$(mvn -s $HOME/.m2/settings-snapshots.xml -q -DforceStdout help:evaluate -Dexpression=version.oracle.driver ${PROFILE_PROD})
ORACLE_ARTIFACT_DIR="${HOME}/oracle-libs/${ORACLE_ARTIFACT_VERSION}.0"

pushd ${ORACLE_ARTIFACT_DIR}
mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=ojdbc8.jar
mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=xstreams.jar
popd


# Run tests
if [[ ${ORACLE_VERSION} =~ .*noncdb$ ]] ; then
    MVN_PROP_PDB_NAME='-Ddatabase.pdb.name='
    MVN_PROP_DATABASE_NAME='-Ddatabase.dbname=ORCLCDB'
    MVN_PROP_USER_NAME='dbzuser'
else
    MVN_PROP_USER_NAME='c##dbzuser'
fi

mvn clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-oracle -am -fae \\
    -Poracle-tests                              \\
    -Ddatabase.hostname=0.0.0.0                 \\
    -Ddatabase.admin.hostname=0.0.0.0           \\
    -Dinstantclient.dir=${HOME}/oracle-libs     \\
    -Dmaven.test.failure.ignore=true            \\
    -Dinsecure.repositories=WARN                \\
    -Ddatabase.user=${MVN_PROP_USER_NAME}       \\
    ${MVN_PROP_PDB_NAME}                        \\
    ${MVN_PROP_DATABASE_NAME}                   \\
    -Papicurio                                  \\
    ${PROFILE_PROD}

# Cleanup
docker stop $(docker ps -a -q) || true
docker rm $(docker ps -a -q) || true
    
RESULTS_FOLDER=final-results
RESULTS_PATH=$RESULTS_FOLDER/results

mkdir -p $RESULTS_PATH
cp **/target/surefire-reports/*.xml $RESULTS_PATH
cp **/target/failsafe-reports/*.xml $RESULTS_PATH
rm -rf $RESULTS_PATH/failsafe-summary.xml
tar czf archive.tar.gz $RESULTS_PATH

docker login quay.io -u "$QUAY_USERNAME" -p "$QUAY_PASSWORD"

./jenkins-jobs/scripts/report.sh --connector true --env-file env-file.env --results-folder $RESULTS_FOLDER --attributes "$ATTRIBUTES"
''')
    }
}
