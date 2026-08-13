return { parametersContext ->
    parametersContext.with {
        stringParam(
                'SOURCE_REPOSITORIES',
                'debezium#github.com/debezium/debezium.git cassandra#github.com/debezium/debezium-connector-cassandra.git cockroachdb#github.com/debezium/debezium-connector-cockroachdb.git db2#github.com/debezium/debezium-connector-db2.git ibmi#github.com/debezium/debezium-connector-ibmi.git informix#github.com/debezium/debezium-connector-informix.git ingres#github.com/debezium/debezium-connector-ingres.git spanner#github.com/debezium/debezium-connector-spanner.git vitess#github.com/debezium/debezium-connector-vitess.git tidb#github.com/debezium/debezium-connector-tidb.git yashandb#github.com/debezium/debezium-connector-yashandb.git quarkus#github.com/debezium/debezium-quarkus.git server#github.com/debezium/debezium-server.git operator#github.com/debezium/debezium-operator.git platform#github.com/debezium/debezium-platform.git#debezium-platform-conductor#main',
                'A space separated list of additional repositories from which Debezium incubating components are built (id#repo[#directory#branch])'
        )
        stringParam('SOURCE_BRANCH', 'main', 'A branch from which Debezium connectors are built, can be overridden in SOURCE_REPOSITORIES')
        stringParam('IMAGES_REPOSITORY', 'github.com/debezium/container-images.git', 'Repository with Debezium Dockerfiles')
        stringParam('DESCRIPTOR_REPOSITORY', 'github.com/debezium/debezium-descriptors-registry.git', 'Repository where Debezium descriptors are published')
        stringParam('DESCRIPTOR_BRANCH', 'main', 'Branch where descriptors are published for releases')
        stringParam('IMAGES_BRANCH', 'main', 'Branch used for images repository')
        stringParam('MULTIPLATFORM_PLATFORMS', 'linux/amd64,linux/arm64', 'Which platforms to build images for')
    }
}
