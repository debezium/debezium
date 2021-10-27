// Job definition to test PostgreSQL connector against different PostgreSQL versions

freeStyleJob('release-debezium-nightly-image') {

    displayName('Debezium Nightly Image')
    description('Build and deploy nightly image to the registry')
    label('Slave')

    properties {
        githubProjectUrl('https://github.com/debezium/docker-images')
    }

    logRotator {
        daysToKeep(7)
    }

    wrappers {
        timeout {
            noActivity(600)
        }
        credentialsBinding {
            usernamePassword('DOCKER_USERNAME', 'DOCKER_PASSWORD', 'debezium-dockerhub')
        }
    }

    triggers {
        upstream('release-deploy-snapshots')
    }

    publishers {
        mailer('jpechane@redhat.com', false, true)
    }

    parameters {
        stringParam('DEBEZIUM_REPOSITORY', 'debezium/debezium', 'Repository from which Debezium is built')
        stringParam('DEBEZIUM_BRANCH', 'main', 'Branch used to build Debezium')
        stringParam('IMAGES_REPOSITORY', 'https://github.com/debezium/docker-images.git', 'Repository with Debezium Dockerfiles')
        stringParam('IMAGES_BRANCH', 'main', 'Branch used for images repository')
    }

   scm {
        git('$IMAGES_REPOSITORY', '$IMAGES_BRANCH')
    }

    steps {
        shell(readFileFromWorkspace('jenkins-jobs/scripts/trigger-nightly-docker.sh'))
    }
}
