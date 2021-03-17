// Job definition to test PostgreSQL connector against different PostgreSQL versions

freeStyleJob('tools-sync-images') {

    displayName('Synchronize Debezium Images')
    description('Synchronizes Debezium images from Docker Hub to Quay')
    label('Slave')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        daysToKeep(7)
    }

    wrappers {
        timeout {
            noActivity(90)
        }

        credentialsBinding {
            string('DEST_CREDENTIALS', 'debezium-quay')
        }
    }

    triggers {
        cron('H 11 * * 1-5')
    }

    publishers {
        mailer('jpechane@redhat.com', false, true)
    }


    steps {
        shell('''
docker run -e DEST_CREDENTIALS="$DEST_CREDENTIALS" quay.io/debezium/sync-images
''')
    }
}
