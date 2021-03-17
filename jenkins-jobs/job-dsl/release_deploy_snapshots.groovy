pipelineJob('release-deploy-snapshots') {
    displayName('Debezium Deploy Snapshots')
    description('Deploy -SNAPSHOT versions to Maven Central')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        daysToKeep(7)
    }

    triggers {
        cron('0 */6 * * *')
    }

    parameters {

        stringParam('DEBEZIUM_REPOSITORY', 'github.com/debezium/debezium.git', 'Repository from which Debezium is built')
        stringParam('DEBEZIUM_BRANCH', 'master', 'A branch from which Debezium is built')
        stringParam(
                'DEBEZIUM_ADDITIONAL_REPOSITORIES',
                'db2#github.com/debezium/debezium-connector-db2.git#main vitess#github.com/debezium/debezium-connector-vitess.git#master cassandra#github.com/debezium/debezium-connector-cassandra.git#main',
                'A space separated list of additional repositories from which Debezium connectors are built (id#repo#branch)'
        )
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/deploy-snapshots.groovy'))
        }
    }
}
