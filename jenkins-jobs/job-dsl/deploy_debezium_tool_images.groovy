pipelineJob('release-deploy-debezium-tool-images') {
    displayName('Debezium Deploy Tool Images')
    description('Build and deploy debezium tool images to the registry')

    properties {
        githubProjectUrl('https://github.com/debezium/container-images')
    }

    logRotator {
        daysToKeep(7)
        numToKeep(10)
    }

    triggers {
        cron('0 0 * * 1')
    }

    parameters {
        stringParam('MAIL_TO', 'jpechane@redhat.com')
        stringParam('IMAGES_REPOSITORY', 'github.com/debezium/container-images.git', 'Repository with Debezium Dockerfiles')
        stringParam('IMAGES_BRANCH', 'main', 'Branch used for images repository')
        stringParam('TAG', 'latest', 'Tag used for building images')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/build_debezium_tool_images_pipeline.groovy'))
        }
    }
}
