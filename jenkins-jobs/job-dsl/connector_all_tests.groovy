pipelineJob('connector-all-tests') {
    displayName('Debezium All Connectors Test')
    description('Executes tests for all connectors')

    parameters {
        booleanParam('DB2_TEST', true, 'Run DB2 Tests')
        booleanParam('MONGODB_TEST', true, 'Run MongoDB Tests')
        booleanParam('MYSQL_TEST', true, 'Run MySQL Tests')
        booleanParam('ORACLE_TEST', true, 'Run Oracle Tests')
        booleanParam('POSTGRESQL_TEST', true, 'Run PostgreSQL Tests')
        booleanParam('SQLSERVER_TEST', true, 'Run SQL Server Tests')

        stringParam('REPOSITORY_CORE', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', 'main', 'A branch/tag from which Debezium is built')

        // db2 specific
        stringParam('REPOSITORY_DB2', 'https://github.com/debezium/debezium-connector-db2', 'Repository from which DB2 connector is built')

        stringParam('SOURCE_URL', "", "URL to productised sources")
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')

        //oracle specific
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', 'Quay.io credentials id')

        stringParam('LABEL', "", 'Label/Debezium Version')

    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/connector_tests_trigger_pipeline.groovy'))
            sandbox()
        }
    }
}
