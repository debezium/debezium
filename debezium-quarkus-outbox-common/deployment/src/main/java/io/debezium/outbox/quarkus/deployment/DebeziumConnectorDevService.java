/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang3.Validate;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.arc.deployment.ValidationPhaseBuildItem;
import io.quarkus.datasource.common.runtime.DataSourceUtil;
import io.quarkus.datasource.runtime.DataSourceBuildTimeConfig;
import io.quarkus.datasource.runtime.DataSourcesBuildTimeConfig;
import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.BuildSteps;
import io.quarkus.deployment.builditem.CuratedApplicationShutdownBuildItem;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem;
import io.quarkus.deployment.builditem.RunTimeConfigurationDefaultBuildItem;
import io.quarkus.deployment.dev.devservices.GlobalDevServicesConfig;

@BuildSteps(onlyIfNot = IsNormal.class, onlyIf = GlobalDevServicesConfig.Enabled.class)
public class DebeziumConnectorDevService {

    // https://github.com/debezium/container-images/tree/main/examples
    // https://github.com/debezium/debezium-examples/tree/main
    public static final List<String> DEBEZIUM_DATA_SOURCES_DOCKER_IMAGE = List.of("quay.io/debezium/postgres",
            "quay.io/debezium/example-", "debezium/sql-server-keys", "mariadb", "mongo", "mysql");

    private static final String DEBEZIUM_CONNECT_IMAGE = "quay.io/debezium/connect:";

    DebeziumOutboxCommonConfig debeziumOutboxCommonConfig;

    @BuildStep
    void startDebeziumContainer(final DataSourcesBuildTimeConfig dataSourcesBuildTimeConfig,
                                final List<DevServicesResultBuildItem> devServicesResultBuildItems,
                                final BuildProducer<ValidationPhaseBuildItem.ValidationErrorBuildItem> validationErrorBuildItemProducer,
                                final CuratedApplicationShutdownBuildItem curatedApplicationShutdownBuildItem,
                                final BuildProducer<RunTimeConfigurationDefaultBuildItem> runTimeConfigurationDefaultBuildItemProducer) {
        final DataSourceBuildTimeConfig dataSourceBuildTimeConfig = dataSourcesBuildTimeConfig.dataSources().get(DataSourceUtil.DEFAULT_DATASOURCE_NAME);
        final List<ValidationPhaseBuildItem.ValidationErrorBuildItem> validationErrorBuildItems = new ArrayList<>();

        if (dataSourceBuildTimeConfig.devservices().imageName().isEmpty()) {
            validationErrorBuildItems.add(new ValidationPhaseBuildItem.ValidationErrorBuildItem(
                    new IllegalStateException("Missing dev service image name for default datasource")));
        }
        else if (DEBEZIUM_DATA_SOURCES_DOCKER_IMAGE.stream()
                .noneMatch(debeziumDatasourceImage -> dataSourceBuildTimeConfig.devservices().imageName().get().startsWith(debeziumDatasourceImage))) {
            validationErrorBuildItems.add(new ValidationPhaseBuildItem.ValidationErrorBuildItem(
                    new IllegalStateException("Default datasource dev service must define one of these debezium datasource connect image: %s"
                            .formatted(String.join(",", DEBEZIUM_DATA_SOURCES_DOCKER_IMAGE)))));
        }
        if (!debeziumOutboxCommonConfig.devservices().imageName().startsWith(DEBEZIUM_CONNECT_IMAGE)) {
            validationErrorBuildItems.add(new ValidationPhaseBuildItem.ValidationErrorBuildItem(
                    new IllegalStateException("quarkus.debezium-outbox.devservices.image-name is invalid. Image should start with '%s', current image name '%s'"
                            .formatted(DEBEZIUM_CONNECT_IMAGE, debeziumOutboxCommonConfig.devservices().imageName()))));
        }
        if (!validationErrorBuildItems.isEmpty()) {
            validationErrorBuildItems.forEach(validationErrorBuildItemProducer::produce);
        }
        else {
            final HostInternalKafkaBootstrapServer hostInternalKafkaBootstrapServer = devServicesResultBuildItems.stream()
                    // get the kafka RedPanda container
                    .filter(devServicesResultBuildItem -> "kafka-client".equals(devServicesResultBuildItem.getName()))
                    .map(kafkaDevServiceBuildItem -> kafkaDevServiceBuildItem.getConfig().get("kafka.bootstrap.servers"))
                    .flatMap(runtimeKafkaBootstrapServers -> Stream.of(runtimeKafkaBootstrapServers.split(",")))
                    .filter(runtimeKafkaBootstrapServer -> runtimeKafkaBootstrapServer.contains(DebeziumConnectContainer.HOST_DOCKER_INTERNAL))
                    .map(HostInternalKafkaBootstrapServer::new)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Missing host.internal.kafka BootstrapServer"));
            final DebeziumConnectContainer debeziumConnectContainer = new DebeziumConnectContainer(
                    DockerImageName.parse(debeziumOutboxCommonConfig.devservices().imageName()), hostInternalKafkaBootstrapServer);
            debeziumConnectContainer.start();
            curatedApplicationShutdownBuildItem.addCloseTask(debeziumConnectContainer::stop, true);

            final String connectorHostUrl = String.format("http://localhost:%d", debeziumConnectContainer.getDebeziumRestApiHostPort());
            runTimeConfigurationDefaultBuildItemProducer
                    .produce(new RunTimeConfigurationDefaultBuildItem("debezium.quarkus.outbox.connector.host.url", connectorHostUrl));
        }
    }

    public record HostInternalKafkaBootstrapServer(String bootstrapServer) {
        static final Pattern HOST_INTERNAL_KAFKA_BOOTSTRAP_SERVER = Pattern.compile("HOST_DOCKER_INTERNAL://(host\\.docker\\.internal):([0-9]+)");

        public HostInternalKafkaBootstrapServer {
            Objects.requireNonNull(bootstrapServer);
            Validate.validState(HOST_INTERNAL_KAFKA_BOOTSTRAP_SERVER.matcher(bootstrapServer).matches());
        }

        public String toHostPort() {
            final Matcher matcher = HOST_INTERNAL_KAFKA_BOOTSTRAP_SERVER.matcher(bootstrapServer);
            Validate.validState(matcher.find());
            return matcher.group(1) + ":" + matcher.group(2);
        }
    }

}
