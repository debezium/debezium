// Job definition to test MongoDB connector against different MongoDB versions

matrixJob('debezium-mongodb-matrix-test') {

    displayName('Debezium MongoDB Test Matrix')
    description('Executes tests for MongoDB Connector with MongoDB matrix')
    label('Slave')

    axes {
        text('MONGODB_VERSION', '3.2', '3.4', '3.6', '4.0')
    }

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', '*/master', 'A branch/tag from which Debezium is built')
    }

    scm {
        git('$REPOSITORY', '$BRANCH')
    }

    triggers {
        cron('H 04 * * 1-5')
    }

    wrappers {
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
        maven {
            goals('clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-mongodb -am -fae -Dmaven.test.failure.ignore=true -Dversion.mongo.server=$MONGODB_VERSION')
        }
    }
}
