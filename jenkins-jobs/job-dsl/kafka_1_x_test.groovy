// Job definition to test PostgreSQL connector against different PostgreSQL versions

freeStyleJob('debezium-kafka-1.x-test') {

    displayName('Debezium Kafka 1.x Test')
    description('Executes testsuite with Kafka 1.x dependencies')
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
            goals('clean install -U -s $HOME/.m2/settings-snapshots.xml -fae -Dmaven.test.failure.ignore=true -Dpostgres.port=55432 -Dversion.kafka=1.1.1 -Dversion.zookeeper=3.4.10 -Dversion.confluent.platform=4.1.2 -Dmysql.port=33306 -Dmysql.gtid.port=33306 -Dmysql.gtid.replica.port=33306 -Dmysql.replica.port=33306')
        }
    }
}
