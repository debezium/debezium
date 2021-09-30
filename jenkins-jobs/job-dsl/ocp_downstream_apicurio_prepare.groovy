pipelineJob('ocp-downstream-apicurio-prepare-job') {
    displayName('Apicurio deployment preparation')
    description('Republishes apicurio registry images to quay.io and replaces them in deployment descriptor')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
//        QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', 'Quay.io credentials id')
        stringParam('QUAY_ORGANISATION', '', 'Organisation where images are copied')
//        APICURIO CONFIG
        stringParam('APIC_RESOURCES_ARCHIVE_URL', '', 'URL to productised apicurio sources')
        stringParam('APIC_RESOURCES_DEPLOYMENT_DESCRIPTOR', 'install.yaml', 'Descriptor for deployment')
        textParam('APIC_IMAGES', '', 'List of productised apicurio images')
//        DEBEZIUM CONFIG
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'master', 'A branch/tag of Debezium sources')
//        EXTRA CONFIG
        booleanParam('PUSH_IMAGES', true, 'Push images to quay.io')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/downstream_apicurio_prepare_pipeline.groovy'))
            sandbox()
        }
    }
}
