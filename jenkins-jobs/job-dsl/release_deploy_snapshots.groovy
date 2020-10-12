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
        cron('H 03 * * 1-5')
    }

    parameters {

        stringParam('DEBEZIUM_REPOSITORY', 'github.com/debezium/debezium.git', 'Repository from which Debezium is built')
        stringParam('DEBEZIUM_BRANCH', 'master', 'A branch from which Debezium is built')
        stringParam(
                'DEBEZIUM_ADDITIONAL_REPOSITORIES',
                'incubator#github.com/debezium/debezium-incubator.git#master vitess#github.com/debezium/debezium-connector-vitess#master',
                'A space separated list of additional repositories from which Debezium incubating components are built (id#repo#branch)'
        )
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/deploy-snapshots.groovy'))
        }
    }
}
