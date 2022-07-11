pipelineJob('rhel-downstream-prepare-job') {
    displayName('AMQ Streams on RHEL Preparation - Downstream')
    description('Prepares image for AMQ Streams on RHEL')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(10)
    }

    parameters {
        stringParam('MAIL_TO', 'debezium-qe@redhat.com')
//        QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', 'Quay.io credentials id')
        stringParam('QUAY_ORGANISATION', '', 'Organisation where images are copied')
//        RHEL CONFIG
        stringParam('RHEL_IMAGE', 'registry.access.redhat.com/ubi8:latest', 'Base RHEL image')
//        KAFKA CONFIG
        stringParam('KAFKA_URL', '', 'AMQ streams kafka')
        stringParam('DBZ_SCRIPTS_VERSION', '2.0', 'Version of debezium used as source of startup scripts')
//        DEBEZIUM CONFIG
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'main', 'A branch/tag of Debezium sources')
//        IMAGE NAME
        booleanParam('AUTO_TAG', true, 'Use automatically generated tag')
        textParam('EXTRA_IMAGE_TAGS', 'latest', 'List of extra texts tags for multiple images')
//        DEBEZIUM CONNECT IMAGE CONFIG
        textParam('DBZ_CONNECTOR_ARCHIVE_URLS', '', 'List of URLs to productised Debezium connectors')
//        EXTRA CONFIG
        textParam('DBZ_EXTRA_LIBS', '', 'List of extra libraries added to connectors')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/rhel_downstream_prepare_pipeline.groovy'))
            sandbox()
        }
    }
}
