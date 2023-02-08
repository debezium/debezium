pipelineJob('ocp-system-test') {
    displayName('System Tests inside OCP')
    description('Runs System Tests inside OCP')

    parameters {
        stringParam('MAIL_TO', 'debezium-qe@redhat.com')
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')

        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'main', 'A branch/tag of Debezium sources')

        stringParam('PULL_SECRET', 'rh-integration-quay-secret', 'Quay.io OpenShift secret')

        stringParam('DOCKER_TAG', 'latest', 'Docker image tag')
        stringParam('OCP_CREDENTIALS', 'openshift-dbz-creds', 'Jenkins credentials id')
        stringParam('OCP_URL', '', 'Ocp url')

        booleanParam('TEST_APICURIO_REGISTRY', false, 'Run tests with Apicurio Registry and Avro serialization')

        booleanParam('STRIMZI_KC_BUILD', false, 'True -> use artifact server, false -> dbz connect image')
        stringParam('DBZ_CONNECT_IMAGE', '', 'Debezium connect image')
        stringParam('ARTIFACT_SERVER_IMAGE', '', 'Artifact server image')
        stringParam('APICURIO_VERSION', '2.2.3.Final', 'Apicurio version')
        stringParam('KAFKA_VERSION', '', 'Kafka version')

        stringParam('STRIMZI_PREPARE_BUILD_NUMBER', '', 'Downstream preparation build to obtain AMQ streams operator from.' +
                ' Leave empty to install operator from ocp marketplace')
        stringParam('STRZ_CHANNEL', 'stable', 'Update channel for Strimzi operator')
        stringParam('APIC_CHANNEL', '2.x', 'Update channel for Apicurio operator')
        stringParam('TEST_TAGS', '', 'Which test tags to run (empty for all)')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/ocp_system_pipeline.groovy'))
            sandbox()
        }
    }
}
