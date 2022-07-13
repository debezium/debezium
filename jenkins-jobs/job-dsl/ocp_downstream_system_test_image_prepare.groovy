pipelineJob('ocp-debezium-testing-downstream-system-image-prepare') {
    displayName('Debezium TestSuite Image Prepare - Downstream')
    description('Creates a testsuite docker image and uploads it to quay')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(10)
    }

    parameters {
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned into docker image')
        stringParam('DBZ_GIT_BRANCH', 'main', 'A branch/tag of Debezium sources')
        stringParam('TAG', 'latest', 'Docker image tag')
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', 'Quay.io credentials id')

        stringParam('APICURIO_PREPARE_BUILD_NUMBER', '', 'Build from which apicurio operator zip is used. Default lastSuccessful')
        stringParam('STRIMZI_PREPARE_BUILD_NUMBER', '', 'Build from which strimzi operator zip is used. Default lastSuccessful')

    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/downstream_system_test_image_pipeline.groovy'))
        }
    }
}
