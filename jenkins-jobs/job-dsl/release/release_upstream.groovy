folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

pipelineJob('release/release-debezium-upstream') {
    displayName('Debezium Release')
    description('Builds Debezium and deploys into Maven Central and Docker Hub')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(5)
    }

    parameters {
        stringParam('MAIL_TO', 'jpechane@redhat.com')
        stringParam('RELEASE_VERSION', 'x.y.z.Final', 'Version of Debezium to be released - e.g. 0.5.2.Final')
        booleanParam('LATEST_SERIES', false, 'Whether this is the latest release series')
        stringParam('DEVELOPMENT_VERSION', 'x.y.z-SNAPSHOT', 'Next development version - e.g. 0.5.3-SNAPSHOT')
        stringParam('DEBEZIUM_REPOSITORY', 'github.com/debezium/debezium.git', 'Repository from which Debezium is built')
        stringParam('DEBEZIUM_BRANCH', 'main', 'A branch from which Debezium is built')
        stringParam(
                'DEBEZIUM_ADDITIONAL_REPOSITORIES',
                'jdbc#github.com/debezium/debezium-connector-jdbc.git#main spanner#github.com/debezium/debezium-connector-spanner.git#main db2#github.com/debezium/debezium-connector-db2.git#main vitess#github.com/debezium/debezium-connector-vitess.git#main cassandra#github.com/debezium/debezium-connector-cassandra.git#main informix#github.com/debezium/debezium-connector-informix.git#main ibmi#github.com/debezium/debezium-connector-ibmi.git#main server#github.com/debezium/debezium-server.git#main operator#github.com/debezium/debezium-operator.git#main',
                'A space separated list of additional repositories from which Debezium incubating components are built (id#repo#branch)'
        )
        stringParam('IMAGES_REPOSITORY', 'github.com/debezium/container-images.git', 'Repository from which Debezium images are built')
        stringParam('IMAGES_BRANCH', 'main', 'A branch from which Debezium images are built')
        stringParam('MULTIPLATFORM_PLATFORMS', 'linux/amd64', 'Which platforms to build images for')
        stringParam('POSTGRES_DECODER_REPOSITORY', 'github.com/debezium/postgres-decoderbufs.git', 'Repository from which PostgreSQL decoder plugin is built')
        stringParam('POSTGRES_DECODER_BRANCH', 'main', 'A branch from which Debezium images are built PostgreSQL decoder plugin is built')
        stringParam('UI_REPOSITORY', 'github.com/debezium/debezium-ui.git', 'Repository from which Debezium UI is built')
        stringParam('UI_BRANCH', 'main', 'A branch from which Debezium UI is built')
        booleanParam('DRY_RUN', true, 'When checked the changes and artifacts are not pushed to repositories and registries')
        booleanParam('IGNORE_SNAPSHOTS', false, 'When checked, snapshot dependencies are allowed to be released; otherwise build fails')
        stringParam('MAVEN_CENTRAL_SYNC_TIMEOUT', '12', 'Timeout in hours to wait for artifacts being published in the Maven Central')
        booleanParam('CHECK_BACKPORTS', false, 'When checked the back ports between the two provided versions will be compared')
        stringParam('BACKPORT_FROM_TAG', 'vx.y.z.Final', 'Tag where back port checks begin - e.g. v1.8.0.Final')
        stringParam('BACKPORT_TO_TAG', 'vx.y.z.Final', 'Tag where back port checks end - e.g. v1.8.1.Final')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release/release-pipeline.groovy'))
        }
    }
}
