pipelineJob('release-debezium-upstream') {
    displayName('Debezium Release')
    description('Builds Debezium and deploys into Maven Central and Docker Hub')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(5)
    }

    parameters {
        stringParam('RELEASE_VERSION', 'x.y.z.Final', 'Version of Debezium to be released - e.g. 0.5.2.Final')
        stringParam('DEVELOPMENT_VERSION', 'x.y.z-SNAPSHOT', 'Next development version - e.g. 0.5.3-SNAPSHOT')
        stringParam('DEBEZIUM_REPOSITORY', 'github.com/debezium/debezium.git', 'Repository from which Debezium is built')
        stringParam('DEBEZIUM_BRANCH', 'master', 'A branch from which Debezium is built')
        stringParam(
                'DEBEZIUM_ADDITIONAL_REPOSITORIES',
                'incubator#github.com/debezium/debezium-incubator.git#master vitess#github.com/debezium/debezium-connector-vitess#master',
                'A space separated list of additional repositories from which Debezium incubating components are built (id#repo#branch)'
        )
        stringParam('IMAGES_REPOSITORY', 'github.com/debezium/docker-images.git', 'Repository from which Debezium images are built')
        stringParam('IMAGES_BRANCH', 'master', 'A branch from which Debezium images are built')
        stringParam('POSTGRES_DECODER_REPOSITORY', 'github.com/debezium/postgres-decoderbufs.git', 'Repository from which PostgreSQL decoder plugin is built')
        stringParam('POSTGRES_DECODER_BRANCH', 'master', 'A branch from which Debezium images are built PostgreSQL decoder plugin is built')
        booleanParam('DRY_RUN', true, 'When checked the changes and artifacts are not pushed to repositories and registries')
        stringParam('MAVEN_CENTRAL_SYNC_TIMEOUT', '12', 'Timeout in hours to wait for artifacts being published in the Maven Central')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release-pipeline.groovy'))
        }
    }
}
