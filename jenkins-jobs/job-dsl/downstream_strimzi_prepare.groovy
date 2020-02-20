pipelineJob('downstream-strimzi-prepare-job') {
    displayName('AMQ Stream Deployment Preparation')
    description('Prepares images and deployment descriptor for AMQ Streams')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
//        QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', "Quay.io credentials id")
        stringParam('QUAY_ORGANISATION', '', "Organisation where images are copied")
//        STRIMZI CONFIG
        stringParam('STRZ_RESOURCES_ARCHIVE_URL', "", "URL to productised strimzi sources")
        textParam('STRZ_IMAGES', "", "List of productised strimzi images")
//        DEBEZIUM CONFIG
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'master', 'A branch/tag of Debezium sources')
//        DEBEZIUM CONNECT IMAGE CONFIG
        booleanParam('DBZ_CONNECT_BUILD', true, 'Also build debezium images')
        textParam('DBZ_CONNECTOR_ARCHIVE_URLS', "", "List of URLs to productised Debezium connectors")
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/downstream_prepare_pipeline.groovy'))
            sandbox()
        }
    }
}
