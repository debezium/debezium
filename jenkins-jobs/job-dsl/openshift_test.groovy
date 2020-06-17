pipelineJob('debezium-openshift-test') {
    displayName('Debezium OpenShift TestSuite')
    description('Executes tests for OpenShift & Strimzi compatibility verification')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')
//        OCP CONFIG
        stringParam('OCP_URL', "", "OpenShift admin API url")
        stringParam('OCP_CREDENTIALS', "openshift-dbz-creds", "Jenkins credentials id")
//        QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'debezium-quay-creds', "Quay.io credentials id")
//        PULL SECRET
        stringParam('PULL_SECRET', 'rh-integration-quay-secret', "Quay.io OpenShift secret")
//        DEBEZIUM CONFIG
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'master', 'A branch/tag of Debezium sources')
        stringParam('DBZ_CONNECT_IMAGE', "", "Kafka Connect Image with DBZ plugins.")
//        STRIMZI CONFIG
        stringParam('STRZ_GIT_REPOSITORY', 'https://github.com/strimzi/strimzi-kafka-operator.git', 'Repository from which Strimzi is cloned')
        stringParam('STRZ_GIT_BRANCH', 'master', 'A branch/tag from which Debezium is built')
        stringParam('STRZ_RESOURCES_ARCHIVE_URL', "", "URL to productised strimzi sources")
//        TEST CONFIG
        stringParam('TEST_WAIT_SCALE', "1", "Wait time scaling factor")
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/openshift_pipeline.groovy'))
            sandbox()
        }
    }
}
