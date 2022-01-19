multiJob('asd-connector-debezium-multi-test') {

    displayName('ASD Debezium Connector Tests Test')
    description('Executes all tests for Database Connectors')
    label('Slave')

    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', 'main', 'A branch/tag from which Debezium is built')
        stringParam('SOURCE_URL', "", "URL to productised sources")
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')
    }

    steps {
        phase('Run') {
            // parallel is default
            phaseJob('connector_mysql_matrix') {
                parameters {
                    currentBuild()
                }
            }

        }
    }
}