freeStyleJob('maven-artifact-check') {
    displayName('Maven artifact check')
    description('Verify integrity of maven artifact')
    label('Slave')

    parameters {
        stringParam('ARTIFACT_URL', '', 'Artifact URL')
        textParam('COMPONENTS', 'debezium-connector-db2 debezium-connector-mongodb debezium-connector-mysql debezium-connector-oracle debezium-connector-postgres debezium-connector-sqlserver debezium-scripting', 'Space separated list of components in artifact')
    }

    steps {
        shell('''
            jenkins-jobs/scripts/maven-artifact-check.sh -u $ARTIFACT_URL --components $COMPONENTS
        ''')
    }
}