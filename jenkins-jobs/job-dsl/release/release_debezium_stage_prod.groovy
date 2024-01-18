folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

pipelineJob('release/release-debezium-stage-prod') {
    displayName('Debezium Build Product Artifacts')
    description('Builds and uploads product artifacts into a stage location')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(5)
    }

    parameters {
        stringParam('MAIL_TO', 'jpechane@redhat.com')
        stringParam('PRODUCT_VERSION', '', 'Product version (e.g. 1.6)')
        stringParam('PRODUCT_VERSION_RELEASE', '', 'Product micro version (e.g. 2) - usually same as Debezium micro version')
        stringParam('PRODUCT_MILESTONE', '', 'Product milestone (e.g. CR1/CR2/GA/...)')
        stringParam('DEBEZIUM_VERSION', '', 'Debezium version (e.g. 1.6.2.Final)')
        stringParam('BACON_VERSION', '2.1.10', 'The bacon version to use')
        stringParam('BACON_CONFIG_URL', '', 'The URL containing bacon config files')
        booleanParam('TEMPORARY_BUILD', false, 'When check a temproray builds are run')
        stringParam('USERNAME', '', 'Username to log to staging host')
        nonStoredPasswordParam('PASSWORD', 'Password to log to staging host')
        booleanParam('STAGE_FILES', false, 'When checked the uploaded artifacts are staged')
        stringParam('TARGET_HOST', 'debezium-staging-host', 'Name of the credentials containing staging host url')
        stringParam('ARTIFACT_DIR', '/mnt/rcm-guest/staging/amq', 'Staging directory')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release/prod-release-pipeline.groovy'))
        }
    }
}
