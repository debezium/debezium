pipelineJob('ocp-debezium-testing-system') {
    displayName('Debezium System-level TestSuite')
    description('Executes tests for OpenShift & Strimzi compatibility verification')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
        stringParam('MAIL_TO', 'jcechace@redhat.com')
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')
        booleanParam('TEST_APICURIO_REGISTRY', false, 'Run tests with Apicurio Registry and Avro serialization')
//        OCP CONFIG
        stringParam('OCP_URL', '', 'OpenShift admin API url')
        stringParam('OCP_CREDENTIALS', 'openshift-dbz-creds', 'Jenkins credentials id')
//        QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'debezium-quay-creds', 'Quay.io credentials id')
//        PULL SECRET
        stringParam('PULL_SECRET', 'rh-integration-quay-secret', 'Quay.io OpenShift secret')
//        DEBEZIUM CONFIG
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'main', 'A branch/tag of Debezium sources')
        stringParam('DBZ_CONNECT_IMAGE', '', 'Kafka Connect Strimzi Image with DBZ plugins.')
        stringParam('DBZ_CONNECT_RHEL_IMAGE', '', 'Kafka Connect RHEL Image with DBZ plugins.')
//        STRIMZI CONFIG
        stringParam('STRZ_GIT_REPOSITORY', 'https://github.com/strimzi/strimzi-kafka-operator.git', 'Repository from which Strimzi is cloned')
        stringParam('STRZ_GIT_BRANCH', 'main', 'A branch/tag from which Debezium is built')
        stringParam('STRZ_RESOURCES_ARCHIVE_URL', '', 'URL to productised strimzi sources')
//        TEST CONFIG
        stringParam('TEST_WAIT_SCALE', '1', 'Wait time scaling factor')
        stringParam('TEST_VERSION_KAFKA', '', 'Kafka version')
        stringParam('TEST_TAGS', '', 'Which test tags to run (empty for all)')
        stringParam('TEST_TAGS_EXCLUDE', '', 'Which test tags to skip (empty for none)')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/openshift_pipeline.groovy'))
            sandbox()
        }
    }
}
