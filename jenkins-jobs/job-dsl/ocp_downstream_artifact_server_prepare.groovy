pipelineJob('ocp-downstream-artifact-server-prepare-job') {
    displayName('Artifact Server Preparation')
    description('Prepares plugins file for artifact server')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
//        QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', 'Quay.io credentials id')
        stringParam('QUAY_ORGANISATION', '', 'Organisation where images are copied')
//        DEBEZIUM CONFIG
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'master', 'A branch/tag of Debezium sources')
//        DEBEZIUM CONNECT IMAGE CONFIG
        textParam('DBZ_CONNECTOR_ARCHIVE_URLS', '', 'List of URLs to productised Debezium connectors')
//        EXTRA CONFIG
        textParam('DBZ_EXTRA_LIBS', '', 'List of extra libraries added to connectors')
//        IMAGE NAME
        booleanParam('AUTO_TAG', true, 'Use automatically generated tag')
        textParam('EXTRA_IMAGE_TAGS', 'latest', 'List of extra texts tags for multiple images')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/downstream_artifact_server_prepare_pipeline.groovy'))
            sandbox()
        }
    }
}
