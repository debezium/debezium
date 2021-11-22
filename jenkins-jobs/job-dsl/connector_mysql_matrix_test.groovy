// Job definition to test MySQL connector against different MySQL versions

matrixJob('connector-debezium-mysql-matrix-test') {

    displayName('Debezium MySQL Connector Test Matrix')
    description('Executes tests for MySQL Connector with MySQL matrix')
    label('Slave')

    axes {
        text('MYSQL_VERSION', '8.0.20', '5.5', '5.6')
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

# Run maven build
mvn clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-mysql -am -fae \
    -Dmaven.test.failure.ignore=true \
    -Dversion.mysql.server=$MYSQL_VERSION \
    -Dmysql.port=4301 \
    -Dmysql.replica.port=4301 \
    -Dmysql.gtid.port=4302 \
    -Dmysql.gtid.replica.port=4303 \
    -P$PROFILE \
    -Dinsecure.repositories=WARN \
    $PROFILE_PROD
''')
    }
}
