// Job definition to test PostgreSQL connector against different PostgreSQL versions

matrixJob('debezium-postgresql-matrix-test') {

    displayName('Debezium PostgreSQL Test Matrix')
    description('Executes tests for PostgreSQL Connector with PostgreSQL matrix')
    label('Slave')
    combinationFilter('''
         DECODER_PLUGIN == "decoderbufs" ||
        (DECODER_PLUGIN == "pgoutput" && (POSTGRES_VERSION == "10" || POSTGRES_VERSION == "11")) ||
         POSTGRES_VERSION == "12"
         ''')

    axes {
        text('POSTGRES_VERSION', '10', '11', '12')
        text('DECODER_PLUGIN', 'decoderbufs', 'wal2json', 'pgoutput')
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
        environmentVariables {
            groovy(
                '''
                if (POSTGRES_VERSION.endsWith('alpine')) {
                ['MAVEN_ARGS': '-Dpostgres.config.file=/usr/local/share/postgresql/postgresql.conf.sample']
                }
                else {
                ['MAVEN_ARGS': '-Dnone']
                }
                '''
            )
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
            goals('clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-postgres -am -fae -Dmaven.test.failure.ignore=true -Dpostgres.port=55432 -Dversion.postgres.server=$POSTGRES_VERSION -Ddecoder.plugin.name=$DECODER_PLUGIN -Dtest.argline="-Ddebezium.test.records.waittime=5" $MAVEN_ARGS')
        }
    }
}
