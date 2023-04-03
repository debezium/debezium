folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

pipelineJob('release/release-deploy-container-images') {
    displayName('Debezium Deploy Container Images')
    description('Build and deploy Container images to the registry')

    properties {
        githubProjectUrl('https://github.com/debezium/container-images')
    }

    logRotator {
        daysToKeep(7)
        numToKeep(10)
    }

    triggers {
        cron('@midnight')
    }

    parameters {
        stringParam('MAIL_TO', 'jpechane@redhat.com')
        stringParam('DEBEZIUM_REPOSITORY', 'debezium/debezium', 'Repository from which Debezium is built')
        stringParam('IMAGES_REPOSITORY', 'github.com/debezium/container-images.git', 'Repository with Debezium Dockerfiles')
        stringParam('IMAGES_BRANCH', 'main', 'Branch used for images repository')
        stringParam('STREAMS_TO_BUILD_COUNT', '2', 'How many most recent streams should be built')
        stringParam('TAGS_PER_STREAM_COUNT', '1', 'How any most recent tags per stream should be built')
        stringParam('MULTIPLATFORM_PLATFORMS', 'linux/amd64,linux/arm64', 'Which platforms to build images for')
        booleanParam('SKIP_UI', false, 'Should UI image be skipped?')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release/build_debezium_images_pipeline.groovy'))
        }
    }
}
