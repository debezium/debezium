pipelineJob('ocp-system-test') {
    displayName('System Tests inside OCP')
    description('Runs System Tests inside OCP')

    parameters {
        stringParam('MAIL_TO', 'jcechace@redhat.com')
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')

        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'main', 'A branch/tag of Debezium sources')

        stringParam('PULL_SECRET', 'rh-integration-quay-secret', 'Quay.io OpenShift secret')
        stringParam('DOCKER_TAG', 'latest', 'Docker image tag')
        stringParam('OCP_PROJECT_NAME', 'debezium-test', 'OCP projects name')
        stringParam('OCP_CREDENTIALS', 'openshift-dbz-creds', 'Jenkins credentials id')
        stringParam('OCP_URL', '', 'Ocp url')

        booleanParam('TEST_APICURIO_REGISTRY', false, 'Run tests with Apicurio Registry and Avro serialization')

        booleanParam('STRIMZI_KC_BUILD', false, 'Use connect image instead of artifact server')
        stringParam('DBZ_CONNECT_IMAGE', 'quay.io/rh_integration/test-strimzi-kafka:strz-latest-kafka-3.1.0-apc-2.2.3.Final-dbz-2.0.0-SNAPSHOT', 'Debezium connect image')
        stringParam('ARTIFACT_SERVER_IMAGE', 'quay.io/rh_integration/test-artifact-server:2.0.0-SNAPSHOT', 'Artifact server image')
        stringParam('APICURIO_VERSION', '2.2.3.Final', 'Apicurio version')

        stringParam('STRZ_GIT_REPOSITORY', 'https://github.com/strimzi/strimzi-kafka-operator.git', 'Repository from which Strimzi is cloned')
        stringParam('STRZ_GIT_BRANCH', 'main', 'A branch/tag from which Strimzi is built')
        stringParam('STRZ_DOWNSTREAM_URL', '', 'URL to productised strimzi sources')

        stringParam('APIC_GIT_REPOSITORY', 'https://github.com/Apicurio/apicurio-registry-operator.git', 'Repository from which Apicurio is cloned')
        stringParam('APIC_GIT_BRANCH', 'main', 'A branch/tag from which Apicurio is built')
        stringParam('APIC_DOWNSTREAM_URL', '', 'URL to productised apicurio sources')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/ocp_system_pipeline.groovy'))
            sandbox()
        }
    }
}