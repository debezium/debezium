pipelineJob('ocp-upstream-kafka-connect-prepare-job') {
    displayName('Kafka Connect Image Preparation - Upstream')
    description('Prepares Strimzi')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(10)
    }

    parameters {
        stringParam('MAIL_TO', 'debezium-qe@redhat.com')
//        QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'debezium-quay-creds', 'Quay.io credentials id')
        stringParam('QUAY_ORGANISATION', 'debezium', 'Organisation where images are copied')
//        DEBEZIUM CONFIG
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'main', 'A branch/tag of Debezium sources')
        stringParam('DBZ_GIT_REPOSITORY_DB2', 'https://github.com/debezium/debezium-connector-db2.git', 'Repository from which Debezium DB2 sources are cloned')
        stringParam('DBZ_GIT_BRANCH_DB2', 'main', 'A branch/tag of Debezium DB2 sources')
//        IMAGE NAME
        booleanParam('AUTO_TAG', true, 'Use automatically generated tag')
        textParam('EXTRA_IMAGE_TAGS', 'latest', 'List of extra texts tags for multiple images')
//        BASE IMAGE TAG
        stringParam('KC_BASE_TAG', 'latest', 'Kafka connect base image tag')
//        COMPONENT VERSIONS
        stringParam('APICURIO_VERSION', '2.1.0.Final', 'Service registry bits version')
        stringParam('KAFKA_VERSION', '3.1.0', 'Kafka version')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/upstream_kafka_connect_prepare_pipeline.groovy'))
            sandbox()
        }
    }
}
