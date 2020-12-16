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
                'incubator#github.com/debezium/debezium-incubator.git#master db2#github.com/debezium/debezium-connector-db2#main vitess#github.com/debezium/debezium-connector-vitess#master cassandra#github.com/debezium/debezium-connector-cassandra#main',
                'A space separated list of additional repositories from which Debezium incubating components are built (id#repo#branch)'
        )
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/deploy-snapshots.groovy'))
        }
    }
}
