pipelineJob('ocp-downstream-trigger-all-prepare') {
    displayName('Trigger Preparations - Downstream')
    description('Triggers downstream preparation jobs')

    parameters {
        stringParam('LABEL', '', 'Optional label for the preparation jobs')
        booleanParam('EXECUTE_AS', false, 'Execute downstream Artifact Server prepare')
        booleanParam('EXECUTE_STRIMZI', false, 'Execute AMQ Stream Deployment Preparation')
        booleanParam('EXECUTE_APICURIO', false, 'Execute Apicurio deployment preparation')
        booleanParam('EXECUTE_RHEL', false, 'Execute AMQ Streams on RHEL Preparation')


        stringParam('MAIL_TO', 'debezium-qe@redhat.com')
//      QUAY CONFIG
        stringParam('QUAY_CREDENTIALS', 'rh-integration-quay-creds', 'Quay.io credentials id')
        stringParam('QUAY_ORGANISATION', '', 'Organisation where images are copied')
//      DEBEZIUM CONFIG
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium.git', 'Repository from which Debezium sources are cloned')
        stringParam('DBZ_GIT_BRANCH', 'main', 'A branch/tag of Debezium sources')
        stringParam('DBZ_GIT_REPOSITORY_DB2', 'https://github.com/debezium/debezium-connector-db2.git', 'Repository from which Debezium DB2 sources are cloned')
        stringParam('DBZ_GIT_BRANCH_DB2', 'main', 'A branch/tag of Debezium DB2 sources')

//      OCP STRIMZI
        stringParam('STRZ_RESOURCES_ARCHIVE_URL', '', 'OCP Downstream strimzi - URL to productised strimzi sources')
        stringParam('STRZ_RESOURCES_DEPLOYMENT_DESCRIPTOR', '060-Deployment-strimzi-cluster-operator.yaml', 'OCP Downstream strimzi - Descriptor for cluster-operator deployment')
        textParam('STRZ_IMAGES', '', 'OCP Downstream strimzi - List of productised strimzi images')
//      DEBEZIUM CONNECT IMAGE CONFIG
        booleanParam('STRZ_DBZ_CONNECT_BUILD', true, 'OCP Downstream strimzi - Also build debezium images')
        textParam('DBZ_CONNECTOR_ARCHIVE_URLS', '', 'OCP Downstream strimzi, Artifact Server - List of URLs to productised Debezium connectors')
        textParam('STRZ_DBZ_EXTRA_LIBS', '', 'Downstream Strimzi, RHEL - List of extra libraries added to connectors')

//      APICURIO
        stringParam('APIC_RESOURCES_ARCHIVE_URL', '', 'URL to productised apicurio sources')
        stringParam('APIC_RESOURCES_DEPLOYMENT_DESCRIPTOR', 'install.yaml', 'Descriptor for deployment')
        booleanParam('PUSH_IMAGES', true, 'Apicurio - Push images to quay.io')
        textParam('APIC_IMAGES', '', 'List of productised apicurio images')


//      RHEL
        booleanParam('AUTO_TAG', true, 'RHEL, Artifact Server - Use automatically generated tag')
        textParam('EXTRA_IMAGE_TAGS', 'latest', 'RHEL - List of extra texts tags for multiple images')
        stringParam('RHEL_IMAGE', 'registry.access.redhat.com/ubi8:latest', 'Base RHEL image')
        stringParam('KAFKA_URL', '', 'RHEL - AMQ streams kafka')


//      ARTIFACT SERVER
        textParam('AS_DBZ_EXTRA_LIBS', '', 'Artifact Server - List of extra libraries added to connectors')
        textParam('AS_EXTRA_IMAGE_TAGS', 'latest', 'Artifact Server - List of extra texts tags for multiple images')
    }
    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/downstream_preparations_trigger_pipeline.groovy'))
            sandbox()
        }
    }
}
