pipelineJob('test-milti-matrix-job') {
    displayName('Test Multi Matrix Job')
    description('Test Multi Matrix Job desc')

    logRotator {
        daysToKeep(7)
        numToKeep(10)
    }

    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('DB2_REPOSITORY', 'https://github.com/debezium/debezium-connector-db2', 'Repository from which db2 connector is built')
        stringParam('BRANCH', 'main', 'A branch/tag from which Debezium is built')
        stringParam('SOURCE_URL', "", "URL to productised sources")
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/multiple-matrixes-test.groovy'))
        }
    }
}
