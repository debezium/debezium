// Job definition to test MongoDB connector against different MongoDB versions

matrixJob('connector-debezium-mongodb-matrix-test') {

    displayName('Debezium MongoDB Connector Test Matrix')
    description('Executes tests for MongoDB Connector with MongoDB matrix')
    label('Slave')

    axes {
        text('MONGODB_VERSION', '3.2', '3.4', '3.6', '4.0', '4.2')
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
    PROFILE_PROD="pnc"
    curl -OJs $SOURCE_URL && unzip debezium-*-src.zip
else
    PROFILE_PROD="none"
    git clone $REPOSITORY . 
    git checkout $BRANCH
fi
                    
# Run maven build
mvn clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-mongodb -am -fae \
    -Dmaven.test.failure.ignore=true \
    -Dversion.mongo.server=$MONGODB_VERSION \
    -Dinsecure.repositories=WARN \
    -P$PROFILE_PROD 
''')
    }
}
