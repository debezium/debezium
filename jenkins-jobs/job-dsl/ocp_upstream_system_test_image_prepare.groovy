pipelineJob('ocp-debezium-testing-system-image-prepare') {
    displayName('Debezium TestSuite Image Prepare')
    description('Creates a testsuite docker image and uploads it to quay')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(10)
    }

    parameters {
        stringParam('PARENT_DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository used to build the docker image')
        stringParam('PARENT_DBZ_GIT_BRANCH', 'main')
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned into docker image')
        stringParam('DBZ_GIT_BRANCH', 'main', 'A branch/tag of Debezium sources')
        stringParam('TAG', 'latest', 'Docker image tag')
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', 'Quay.io credentials id')
        stringParam('PULL_SECRET', 'rh-integration-quay-secret', 'Quay.io OpenShift secret')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/upstream_system_test_image_pipeline.groovy'))
        }
    }
}
