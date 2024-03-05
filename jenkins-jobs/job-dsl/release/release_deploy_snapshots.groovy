folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

pipelineJob('release/release-deploy_snapshots_pipeline') {
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
                'db2#github.com/debezium/debezium-connector-db2.git#main vitess#github.com/debezium/debezium-connector-vitess.git#main cassandra#github.com/debezium/debezium-connector-cassandra.git#main spanner#github.com/debezium/debezium-connector-spanner.git#main jdbc#github.com/debezium/debezium-connector-jdbc.git#main informix#github.com/debezium/debezium-connector-informix.git#main ibmi#github.com/debezium/debezium-connector-ibmi.git#main server#github.com/debezium/debezium-server.git#main operator#github.com/debezium/debezium-operator.git#main',
                'A space separated list of additional repositories from which Debezium connectors are built (id#repo#branch)'
        )
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release/deploy_snapshots_pipeline.groovy'))
        }
    }
}
