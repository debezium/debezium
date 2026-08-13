return { parametersContext ->
    parametersContext.with {
        stringParam('POSTGRES_DECODER_REPOSITORY', 'github.com/debezium/postgres-decoderbufs.git', 'Repository from which PostgreSQL decoder plugin is built')
        stringParam('POSTGRES_DECODER_BRANCH', 'main', 'A branch from which Debezium images are built PostgreSQL decoder plugin is built')
        stringParam('ZULIP_TO', '448915')
        booleanParam('DRY_RUN', true, 'When checked the changes and artifacts are not pushed to repositories and registries')
        booleanParam('FROM_SCRATCH', true, 'When checked if the job is restarted the existing workd directory is cleaned')
        stringParam('RELEASE_VERSION', 'x.y.z.Final', 'Version of Debezium to be released - e.g. 3.4.0.Final')

        stringParam('DEVELOPMENT_VERSION', 'x.y.z-SNAPSHOT', 'Next development version - e.g. 3.4.0-SNAPSHOT')
        booleanParam('LATEST_SERIES', false, 'Whether this is the latest release series')
        booleanParam('IGNORE_SNAPSHOTS', false, 'When checked, snapshot dependencies are allowed to be released; otherwise build fails')
        booleanParam('CHECK_BACKPORTS', false, 'When checked the back ports between the two provided versions will be compared')
        stringParam('BACKPORT_FROM_TAG', 'vx.y.z.Final', 'Tag where back port checks begin - e.g. v1.8.0.Final')
        stringParam('BACKPORT_TO_TAG', 'vx.y.z.Final', 'Tag where back port checks end - e.g. v1.8.1.Final')
    }
}
