pipelineJob('release-debezium-stage-prod') {
    displayName('Debezium Stage Product Artifacts')
    description('Uploads product artifacts into stage location')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(5)
    }

    parameters {
        stringParam('BUILD_VERSION', '', 'Maven artifact id of the product binaries')
        stringParam('BUILD_VERSION_INCUBATOR', '', 'Maven artifact id of the product binaries in incubator')
        stringParam('PRODUCT_VERSION', '', 'Product version')
        stringParam('USERNAME', '', 'Username to log to staging host')
        nonStoredPasswordParam('PASSWORD', 'Password to log to staging host')
        stringParam('CONNECTORS', 'mysql postgres mongodb sqlserver', 'The list of released connectors')
        stringParam('CONNECTORS_INCUBATOR', 'db2', 'The list of released incubating connectors')
        booleanParam('STAGE_FILES', false, 'When checked the uploaded artifacts are staged')
        stringParam('SOURCE_MAVEN_REPO', 'debezium-prod-repo', 'Maven repository URL with product artifacts')
        stringParam('TARGET_HOST', 'debezium-staging-host', 'Name of the credentials containing staging host url')
        stringParam('ARTIFACT_DIR', '/mnt/rcm-guest/staging/amq', 'Staging directory')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/prod-release-pipeline.groovy'))
        }
    }
}
