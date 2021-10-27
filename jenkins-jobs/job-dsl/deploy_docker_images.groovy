pipelineJob('release-deploy-docker-images') {
    displayName('Debezium Deploy Docker Images')
    description('Build and deploy Docker images to the registry')

    properties {
        githubProjectUrl('https://github.com/debezium/docker-images')
    }

    logRotator {
        daysToKeep(7)
    }

    triggers {
        cron('@midnight')
    }

    parameters {
        stringParam('MAIL_TO', 'jpechane@redhat.com')
        stringParam('DEBEZIUM_REPOSITORY', 'debezium/debezium', 'Repository from which Debezium is built')
        stringParam('IMAGES_REPOSITORY', 'github.com/debezium/docker-images.git', 'Repository with Debezium Dockerfiles')
        stringParam('IMAGES_BRANCH', 'main', 'Branch used for images repository')
        stringParam('STREAMS_TO_BUILD_COUNT', '2', 'How many most recent streams should be built')
        stringParam('TAGS_PER_STREAM_COUNT', '1', 'How any most recent tags per stream should be built')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/build-debezium-images.groovy'))
        }
    }
}
