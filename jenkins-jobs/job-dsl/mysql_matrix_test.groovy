// Job definition to test MySQL connector against different MySQL versions

matrixJob('debezium-mysql-matrix-test') {

    displayName('Debezium MySQL Test Matrix')
    description('Executes tests for MySQL Connector with MySQL matrix')
    label('Slave')

    axes {
        text('MYSQL_VERSION', '8.0.20', '5.5', '5.6')
        text('PROFILE', 'none', 'assembly')
    }

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', 'master', 'A branch/tag from which Debezium is built')
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
    PROFILE_PROD="pnc"
    curl -OJs $SOURCE_URL && unzip debezium-*-src.zip
else
    PROFILE_PROD="none"
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
    -P$PROFILE_PROD
''')
    }
}
