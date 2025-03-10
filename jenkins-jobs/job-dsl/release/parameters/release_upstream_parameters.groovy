return { parametersContext ->
    parametersContext.with {
        booleanParam('LATEST_SERIES', false, 'Whether this is the latest release series')
        stringParam('DEVELOPMENT_VERSION', 'x.y.z-SNAPSHOT', 'Next development version - e.g. 0.5.3-SNAPSHOT')
        stringParam('DEBEZIUM_BRANCH', 'main', 'A branch from which Debezium is built')
        stringParam(
                'DEBEZIUM_ADDITIONAL_REPOSITORIES',
                'spanner#github.com/debezium/debezium-connector-spanner.git#main db2#github.com/debezium/debezium-connector-db2.git#main vitess#github.com/debezium/debezium-connector-vitess.git#main cassandra#github.com/debezium/debezium-connector-cassandra.git#main informix#github.com/debezium/debezium-connector-informix.git#main ibmi#github.com/debezium/debezium-connector-ibmi.git#main server#github.com/debezium/debezium-server.git#main operator#github.com/debezium/debezium-operator.git#main platform#github.com/debezium/debezium-platform.git#debezium-platform-conductor#main',
                'A space separated list of additional repositories from which Debezium incubating components are built (id#repo#branch)'
        )
        stringParam('POSTGRES_DECODER_REPOSITORY', 'github.com/debezium/postgres-decoderbufs.git', 'Repository from which PostgreSQL decoder plugin is built')
        stringParam('POSTGRES_DECODER_BRANCH', 'main', 'A branch from which Debezium images are built PostgreSQL decoder plugin is built')
        stringParam('UI_REPOSITORY', 'github.com/debezium/debezium-ui.git', 'Repository from which Debezium UI is built')
        stringParam('UI_BRANCH', 'main', 'A branch from which Debezium UI is built')
        booleanParam('IGNORE_SNAPSHOTS', false, 'When checked, snapshot dependencies are allowed to be released; otherwise build fails')
        stringParam('MAVEN_CENTRAL_SYNC_TIMEOUT', '12', 'Timeout in hours to wait for artifacts being published in the Maven Central')
        booleanParam('CHECK_BACKPORTS', false, 'When checked the back ports between the two provided versions will be compared')
        stringParam('BACKPORT_FROM_TAG', 'vx.y.z.Final', 'Tag where back port checks begin - e.g. v1.8.0.Final')
        stringParam('BACKPORT_TO_TAG', 'vx.y.z.Final', 'Tag where back port checks end - e.g. v1.8.1.Final')
    }
}
