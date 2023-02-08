pipelineJob('ocp-debezium-testing-system') {
    displayName('Debezium System-level TestSuite')
    description('Executes tests for OpenShift & Strimzi compatibility verification')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(10)
    }

    parameters {
        stringParam('MAIL_TO', 'debezium-qe@redhat.com')
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
        stringParam('DBZ_GIT_REPOSITORY_DB2', 'https://github.com/debezium/debezium-connector-db2.git', 'Repository from which Debezium DB2 sources are cloned')
        stringParam('DBZ_GIT_BRANCH_DB2', 'main', 'A branch/tag of Debezium DB2 sources')
//        OPERATORS CONFIG
        stringParam('STRIMZI_PREPARE_BUILD_NUMBER', '', 'Downstream preparation build to obtain AMQ streams operator from.' +
                ' Leave empty to install operator from ocp marketplace')
        stringParam('STRIMZI_OPERATOR_CHANNEL', 'stable', 'Update channel for Strimzi operator')
        stringParam('APICURIO_OPERATOR_CHANNEL', '2.x', 'Update channel for Apicurio operator')
//      Images config
        stringParam('IMAGE_DBZ_AS', '', "Debezium artifact server image (usable with Strimzi's build mechanism")
        stringParam('IMAGE_CONNECT_STRZ', '', 'Kafka Connect Strimzi Image with DBZ plugins.')
        stringParam('IMAGE_CONNECT_RHEL', '', 'Kafka Connect RHEL Image with DBZ plugins.')
//        TEST CONFIG
        stringParam('TEST_WAIT_SCALE', '1', 'Wait time scaling factor')
        stringParam('TEST_TAGS', '', 'Which test tags to run (empty for all)')
//        Artifact Versions
        stringParam('TEST_VERSION_KAFKA', '', 'Kafka version')
        stringParam('AS_VERSION_APICURIO', '', 'Service registry bits version')
        stringParam('AS_VERSION_DEBEZIUM', '', 'Debezium bits version')
//        Maven
        booleanParam('DEBUG_MODE', false, 'Enable remote debugger')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/system_pipeline.groovy'))
        }
    }
}
