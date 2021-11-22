// Job definition to test SQL Server connector against different SQL Server versions

matrixJob('connector-debezium-sqlserver-matrix-test') {

    displayName('Debezium SQL Server Connector Test Matrix')
    description('Executes tests for SQL Server Connector with SQL Server matrix')
    label('Slave')

    axes {
        text('SQL_SERVER_VERSION', '2017', '2019')
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

    scm {
        git('$REPOSITORY', '$BRANCH')
    }

    triggers {
        cron('H 04 * * 1-5')
    }

    wrappers {
        preBuildCleanup()

        timeout {
            noActivity(1200)
        }
    }

    publishers {
        archiveJunit('**/target/surefire-reports/*.xml')
        archiveJunit('**/target/failsafe-reports/*.xml')
        mailer('jpechane@redhat.com', false, true)
    }

    logRotator {
        daysToKeep(7)
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
else
    git clone $REPOSITORY . 
    git checkout $BRANCH
fi

# Select image
case $SQL_SERVER_VERSION in
  "2017") DATABASE_IMAGE="mcr.microsoft.com/mssql/server:2017-latest" ;;
  "2019") DATABASE_IMAGE="mcr.microsoft.com/mssql/server:2019-latest" ;;
   *) status=$status ;;
esac
                    
# Run maven build
mvn clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-sqlserver -am -fae \
    -Dmaven.test.failure.ignore=true \
    -Ddocker.filter=$DATABASE_IMAGE \
    -Dinsecure.repositories=WARN \
    $PROFILE_PROD 
''')
    }
}
