folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

pipelineJob('release/release-debezium-charts-upstream') {
    displayName('Debezium Charts Release')
    description('Packages helm charts push into Quay.io and create Github release')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(5)
    }

    parameters {
        stringParam('MAIL_TO', 'mvitale@redhat.com')
        stringParam('RELEASE_VERSION', 'x.y.z.Final', 'Version of Debezium to be released - e.g. 0.5.2.Final')
        stringParam('DEBEZIUM_OPERATOR_REPOSITORY', 'github.com/debezium/debezium-operator', 'Repository from which Debezium Operator is built')
        stringParam('DEBEZIUM_OPERATOR_BRANCH', 'main', 'A branch from which Debezium Operator is built')
        stringParam('DEBEZIUM_PLATFORM_REPOSITORY', 'github.com/debezium/debezium-platform', 'Repository from which Debezium Platform is built')
        stringParam('DEBEZIUM_PLATFORM_BRANCH', 'main', 'A branch from which Debezium Platform is built')
        stringParam('OCI_ARTIFACT_REPO_URL', 'oci://quay.io/debezium-charts', 'OCI repo URL where helm artifacts will be pushed')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release/release-charts-pipeline.groovy'))
        }
    }
}
