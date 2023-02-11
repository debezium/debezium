// Job definition to test MySQL connector against different MySQL versions

matrixJob('connector-debezium-mysql-matrix-test') {

    displayName('Debezium MySQL Connector Test Matrix')
    description('Executes tests for MySQL Connector with MySQL matrix')
    label('Slave')

    axes {
        text('MYSQL_VERSION', '8.0.20', '5.7')
        text('PROFILE', 'none', 'assembly')
        label("Node", "Slave")
    }

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', 'main', 'A branch/tag from which Debezium is built')
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
        credentialsBinding {
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
        shell('''
# Ensure WS cleaup
ls -A1 | xargs rm -rf

# Retrieve sources
if [ "$PRODUCT_BUILD" == true ] ; then
    export MAVEN_OPTS="-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true"
    PROFILE_PROD="-Ppnc"
    curl -OJs $SOURCE_URL && unzip debezium-*-src.zip
    pushd debezium-*-src
    pushd $(ls | grep -P 'debezium-[^-]+.Final')
    ATTRIBUTES="downstream MySQL $MYSQL_VERSION $PROFILE"
else
    git clone $REPOSITORY . 
    git checkout $BRANCH
    ATTRIBUTES="upstream MySQL $MYSQL_VERSION $PROFILE"
fi

# Run maven build
mvn clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-bom,debezium-connector-mysql -am -fae \
    -Dmaven.test.failure.ignore=true \
    -Dversion.mysql.server=$MYSQL_VERSION \
    -Dmysql.port=4301 \
    -Dmysql.replica.port=4301 \
    -Dmysql.gtid.port=4302 \
    -Dmysql.gtid.replica.port=4303 \
    -P$PROFILE \
    -Papicurio \
    -Dinsecure.repositories=WARN \
    $PROFILE_PROD
    

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
