pipelineJob('connector-debezium-oracle-matrix-test') {
    displayName('Debezium Oracle Connector Test Matrix')
    description('Executes tests for Oracle connector with Oracle matrix')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
        stringParam('MAIL_TO', 'debezium-qe@redhat.com')
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', 'main', 'A branch/tag from which Debezium is built')
//          QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', 'Quay.io credentials id')
//          PRODUCT CONFIG
        stringParam('SOURCE_URL', "", "URL to productised sources")
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')
    }
    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/connector_oracle_matrix_pipeline.groovy'))
            sandbox()
        }
    }
}
