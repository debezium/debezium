/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.testcontainers.DebeziumContainer;

public class SourcePipelineInvocationContextProvider implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {
    public static String KAFKA_CONTAINER_NAME = "kafka";
    public static String KAFKA_CONNECT_CONTAINER_NAME = "connect";
    public static String DATABASE_CONTAINER_NAME = "database";

    private static Logger LOGGER = LoggerFactory.getLogger(SourcePipelineInvocationContextProvider.class);
    private final Map<String, JdbcDatabaseContainer> containers = new HashMap<>();

    private static DockerImageName MYSQL_IMAGE = DockerImageName.parse("debezium/example-mysql").asCompatibleSubstituteFor("mysql");
    private static DockerImageName POSTGRES_IMAGE = DockerImageName.parse("debezium/example-postgres").asCompatibleSubstituteFor("postgres");
    private static DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka");
    private static DockerImageName DEBEZIUM_IMAGE = DockerImageName.parse("debezium/connect");

    private static Network network = Network.newNetwork();
    private static KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("kafka");
    private static DebeziumContainer kafkaConnectContainer = new DebeziumContainer(DEBEZIUM_IMAGE)
            .withKafka(kafkaContainer)
            .withNetwork(network)
            .withNetworkAliases("connect");

    public SourcePipelineInvocationContextProvider() {
        // SetUp MYSQL Container
        // Username and password comes from debezium-example repository
        // @link https://github.com/debezium/debezium-examples
        JdbcDatabaseContainer<?> mySqlContainer = new MySQLContainer<>(MYSQL_IMAGE)
                .withNetwork(network)
                .withUsername("mysqluser")
                .withPassword("debezium")
                .withNetworkAliases("mysql");
        containers.put("mysql", mySqlContainer);

        // SetUp Postgres Container
        // Username and password comes from debezium-example repository
        // @link https://github.com/debezium/debezium-examples
        JdbcDatabaseContainer<?> postgresContainer = new PostgreSQLContainer<>(POSTGRES_IMAGE)
                .withNetwork(network)
                .withUsername("postgres")
                .withPassword("postgres")
                .withNetworkAliases("postgres");
        containers.put("postgres", postgresContainer);

    }

    public static Network getNetwork() {
        return network;
    }

    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext extensionContext) {
        return containers.keySet().stream().map(this::invocationContext);
    }

    private TestTemplateInvocationContext invocationContext(final String database) {
        return new TestTemplateInvocationContext() {

            @Override
            public String getDisplayName(int invocationIndex) {
                return database;
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                final JdbcDatabaseContainer databaseContainer = containers.get(database);
                return Arrays.asList(
                        (BeforeEachCallback) context -> {
                            databaseContainer.start();
                        },
                        (AfterEachCallback) context -> {
                            databaseContainer.stop();
                        },
                        new ParameterResolver() {
                            @Override
                            public boolean supportsParameter(ParameterContext parameterCtx, ExtensionContext extensionCtx) {
                                return true;
                            }

                            @Override
                            public Map<String, GenericContainer<?>> resolveParameter(ParameterContext parameterCtx, ExtensionContext extensionCtx) {
                                Map<String, GenericContainer<?>> containerMap = new HashMap<>();
                                containerMap.put(KAFKA_CONTAINER_NAME, kafkaContainer);
                                containerMap.put(KAFKA_CONNECT_CONTAINER_NAME, kafkaConnectContainer);
                                containerMap.put(DATABASE_CONTAINER_NAME, databaseContainer);

                                return containerMap;
                            }
                        });
            }
        };
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        Stream.of(kafkaContainer, kafkaConnectContainer).forEach(GenericContainer::stop);
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        Startables.deepStart(Stream.of(kafkaContainer, kafkaConnectContainer)).join();
    }
}
