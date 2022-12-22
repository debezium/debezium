pipelineJob('release-deploy_snapshots_pipeline') {
    displayName('Debezium Deploy Snapshots')
    description('Deploy -SNAPSHOT versions to Maven Central')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        daysToKeep(7)
        numToKeep(10)
    }

    triggers {
        cron('0 */6 * * *')
    }

    parameters {

        stringParam('MAIL_TO', 'jpechane@redhat.com')
        stringParam('DEBEZIUM_REPOSITORY', 'github.com/debezium/debezium.git', 'Repository from which Debezium is built')
        stringParam('DEBEZIUM_BRANCH', 'main', 'A branch from which Debezium is built')
        stringParam(
                'DEBEZIUM_ADDITIONAL_REPOSITORIES',
                'db2#github.com/debezium/debezium-connector-db2.git#main vitess#github.com/debezium/debezium-connector-vitess.git#main cassandra#github.com/debezium/debezium-connector-cassandra.git#main spanner#github.com/debezium/debezium-connector-spanner.git#main',
                'A space separated list of additional repositories from which Debezium connectors are built (id#repo#branch)'
        )
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/deploy_snapshots_pipeline.groovy'))
        }
    }
}
