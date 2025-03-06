return { parametersContext ->
    parametersContext.with {
        stringParam('DEBEZIUM_REPOSITORY', 'debezium/debezium', 'Repository from which Debezium is built')
        stringParam('DEBEZIUM_REPOSITORY_URL', 'github.com/debezium/debezium.git', 'Repository from which Debezium is built')
        stringParam('IMAGES_REPOSITORY', 'github.com/debezium/container-images.git', 'Repository with Debezium Dockerfiles')
        stringParam('IMAGES_BRANCH', 'main', 'Branch used for images repository')
        stringParam('MULTIPLATFORM_PLATFORMS', 'linux/amd64,linux/arm64', 'Which platforms to build images for')
    }
}
