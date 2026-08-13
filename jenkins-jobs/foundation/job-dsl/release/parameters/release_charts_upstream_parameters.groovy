return { parametersContext ->
    parametersContext.with {
        stringParam('RELEASE_VERSION', 'x.y.z.Final', 'Version of Debezium to be released - e.g. 0.5.2.Final')
        stringParam('ZULIP_TO', '448915', 'Zulip user ID to send build notifications')
        booleanParam('DRY_RUN', true, 'When checked the changes and artifacts are not pushed to repositories and registries')
        stringParam('DEBEZIUM_OPERATOR_REPOSITORY', 'github.com/debezium/debezium-operator', 'Repository from which Debezium Operator is built')
        stringParam('DEBEZIUM_OPERATOR_BRANCH', 'main', 'A branch from which Debezium Operator is built')
        stringParam('DEBEZIUM_PLATFORM_REPOSITORY', 'github.com/debezium/debezium-platform', 'Repository from which Debezium Platform is built')
        stringParam('DEBEZIUM_PLATFORM_BRANCH', 'main', 'A branch from which Debezium Platform is built')
        stringParam('DEBEZIUM_CHART_REPOSITORY', 'github.com/debezium/debezium-charts', 'Repository from which Debezium Charts is built')
        stringParam('DEBEZIUM_CHART_BRANCH', 'main', 'A branch from which Debezium Charts is built')
        stringParam('OCI_ARTIFACT_REPO_URL', 'oci://quay.io/debezium-charts', 'OCI repo URL where helm artifacts will be pushed')
    }
}
