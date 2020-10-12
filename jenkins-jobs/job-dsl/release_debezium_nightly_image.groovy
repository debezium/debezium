// Job definition to test PostgreSQL connector against different PostgreSQL versions

freeStyleJob('release-debezium-nightly-image') {

    displayName('Debezium Nightly Image')
    description('Triggers a Docker Hub build for a Connect image nightly tag')
    label('Slave')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        daysToKeep(7)
    }

    wrappers {
        timeout {
            noActivity(5)
        }
    }

    triggers {
        upstream('debezium-deploy-snapshots')
    }

    publishers {
        mailer('jpechane@redhat.com', false, true)
    }

    steps {
        shell(readFileFromWorkspace('jenkins-jobs/scripts/trigger-nightly-docker.sh'))
    }
}
