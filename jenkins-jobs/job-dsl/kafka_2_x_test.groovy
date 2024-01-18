// Job definition to test PostgreSQL connector against different PostgreSQL versions

freeStyleJob('debezium-kafka-2.x-test') {

    displayName('Debezium Kafka 2.x Test')
    description('Executes testsuite with Kafka 2.x dependencies')
    label('Slave')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', '*/main', 'A branch/tag from which Debezium is built')
    }

    scm {
        git('$REPOSITORY', '$BRANCH')
    }

    wrappers {
        timeout {
            noActivity(1200)
        }
    }

    publishers {
        archiveArtifacts {
            pattern('**/target/surefire-reports/*.xml')
            pattern('**/target/failsafe-reports/*.xml')
        }
        archiveJunit('**/target/surefire-reports/*.xml') {
            allowEmptyResults()
        }
        archiveJunit('**/target/failsafe-reports/*.xml') {
            allowEmptyResults()
        }
        mailer('jpechane@redhat.com', false, true)
    }

    logRotator {
        daysToKeep(7)
        numToKeep(10)
    }

    steps {
        maven {
            goals('clean install -U -s $HOME/.m2/settings-snapshots.xml -fae -pl -debezium-connect-rest-extension -Dmaven.test.failure.ignore=true -Dpostgres.port=55432 -Dversion.kafka=2.8.1 -Dversion.zookeeper=3.5.9 -Dversion.confluent.platform=6.2.4 -Dmysql.port=33306 -Dmysql.gtid.port=33306 -Dmysql.gtid.replica.port=33306 -Dmysql.replica.port=33306')
        }
    }
}
