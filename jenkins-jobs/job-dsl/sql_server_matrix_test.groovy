// Job definition to test SQL Server connector against different SQL Server versions

matrixJob('debezium-sqlserver-matrix-test') {

    displayName('Debezium SQL Server Test Matrix')
    description('Executes tests for SQL Server Connector with SQL Server matrix')
    label('Slave')

    axes {
        text('VERSION', '2017', '2019')
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

    environmentVariables {
        groovy('''
            def imageVersion = [
                '2017' : 'microsoft/mssql-server-linux:2017-CU9-GDR2',
                '2019': 'mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04'
            ]
            return ['DATABASE_IMAGE': imageVersion[VERSION]]
        ''')
    }

    steps {
        maven {
            goals('clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-sqlserver -am -fae -Dmaven.test.failure.ignore=true -Ddocker.filter=$DATABASE_IMAGE')
        }
    }
}
