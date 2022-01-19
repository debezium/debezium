pipelineJob('connector-multi-test') {
    displayName('Debezium Multi Connector Test Job')
    description('Executes tests for all connectors')
    label('Slave')

    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', 'main', 'A branch/tag from which Debezium is built')
        stringParam('SOURCE_URL', "", "URL to productised sources")
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')

        booleanParam('DB2_TEST', true, 'Run DB2 Tests?')
        booleanParam('MONGODB_TEST', true, 'Run Mongo DB Tests?')
        booleanParam('MYSQL_TEST', true, 'Run MySQL Tests?')
        booleanParam('ORACLE_TEST', true, 'Run Oracle Tests?')
        booleanParam('POSTGRESQL_TEST', true, 'Run PostgreSQL Tests?')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/connector_tests_pipeline.groovy'))
            sandbox()
        }
    }

}
