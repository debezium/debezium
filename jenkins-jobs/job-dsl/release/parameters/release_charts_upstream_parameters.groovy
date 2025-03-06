return { parametersContext ->
    parametersContext.with {
        stringParam('DEBEZIUM_OPERATOR_REPOSITORY', 'github.com/debezium/debezium-operator', 'Repository from which Debezium Operator is built')
        stringParam('DEBEZIUM_OPERATOR_BRANCH', 'main', 'A branch from which Debezium Operator is built')
        stringParam('DEBEZIUM_PLATFORM_REPOSITORY', 'github.com/debezium/debezium-platform', 'Repository from which Debezium Platform is built')
        stringParam('DEBEZIUM_PLATFORM_BRANCH', 'main', 'A branch from which Debezium Platform is built')
        stringParam('OCI_ARTIFACT_REPO_URL', 'oci://quay.io/debezium-charts', 'OCI repo URL where helm artifacts will be pushed')
    }
}
