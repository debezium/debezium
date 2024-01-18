folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

freeStyleJob('release/prepare-maven-artifact-check') {
    displayName('Maven artifact check')
    description('Verify integrity of maven artifact')
    label('Slave')

    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Debezium repository where the script is located')
        stringParam('BRANCH', '*/main', 'A branch/tag where the script is located')
        stringParam('ARTIFACT_URL', '', 'Artifact URL')
        textParam('COMPONENTS', 'debezium-connector-db2 debezium-connector-mongodb debezium-connector-mysql debezium-connector-oracle debezium-connector-postgres debezium-connector-sqlserver debezium-scripting', 'Space separated list of components in artifact')
    }

    scm {
        git('$REPOSITORY', '$BRANCH')
    }

    steps {
        shell('''
            jenkins-jobs/scripts/maven-artifact-check.sh -u $ARTIFACT_URL --components "$COMPONENTS"
        ''')
    }
}
