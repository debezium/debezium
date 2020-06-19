/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import com.jayway.jsonpath.JsonPath;

/**
 * An integration test verifying the Apicurio registry is interoperable with Debezium
 *
 * @author Jiri Pechanec
 */
public class ApicurioRegistryTest {

    private static final String DEBEZIUM_VERSION = "1.2.0.CR1";
    private static final String APICURIO_VERSION = "1.2.2.Final";

    private static final Logger LOGGER = LoggerFactory.getLogger(ApicurioRegistryTest.class);

    private static Network network = Network.newNetwork();

    private static GenericContainer<?> apicurioContainer = new GenericContainer<>("apicurio/apicurio-registry-mem:" + APICURIO_VERSION)
            .withNetwork(network)
            .withExposedPorts(8080)
            .waitingFor(new LogMessageWaitStrategy().withRegEx(".*apicurio-registry-app.*started in.*"));

    private static KafkaContainer kafkaContainer = new KafkaContainer()
            .withNetwork(network);

    public static PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>("debezium/postgres:11")
            .withNetwork(network)
            .withNetworkAliases("postgres");

    public static ImageFromDockerfile apicurioDebeziumImage = new ImageFromDockerfile()
            .withDockerfileFromBuilder(builder -> builder
                    .from("debezium/connect:" + DEBEZIUM_VERSION)
                    .env("KAFKA_CONNECT_DEBEZIUM_DIR", "$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres")
                    .env("APICURIO_VERSION", APICURIO_VERSION)
                    .run("cd $KAFKA_CONNECT_DEBEZIUM_DIR && curl https://repo1.maven.org/maven2/io/apicurio/apicurio-registry-distro-connect-converter/$APICURIO_VERSION/apicurio-registry-distro-connect-converter-$APICURIO_VERSION-converter.tar.gz | tar xzv")
                    .build());

    public static DebeziumContainer debeziumContainer = new DebeziumContainer(apicurioDebeziumImage)
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .dependsOn(kafkaContainer);

    @BeforeClass
    public static void startContainers() {
        Startables.deepStart(Stream.of(
                apicurioContainer, kafkaContainer, postgresContainer, debeziumContainer)).join();
    }

    @Test
    public void shouldConvertToJson() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                Statement statement = connection.createStatement();
                KafkaConsumer<String, String> consumer = getConsumerString(kafkaContainer)) {

            statement.execute("drop schema if exists todo cascade");
            statement.execute("create schema todo");
            statement.execute("create table todo.Todo (id int8 not null, title varchar(255), primary key (id))");
            statement.execute("alter table todo.Todo replica identity full");
            statement.execute("insert into todo.Todo values (1, 'Be Awesome')");
            statement.execute("insert into todo.Todo values (2, 'Learn Quarkus')");

            debeziumContainer.registerConnector("my-connector-json", getConfiguration(1, "io.apicurio.registry.utils.converter.ExtJsonConverter"));

            consumer.subscribe(Arrays.asList("dbserver1.todo.todo"));

            List<ConsumerRecord<String, String>> changeEvents = drain(consumer, 2);

            assertThat(JsonPath.<Integer> read(changeEvents.get(0).key(), "$.payload.id")).isEqualTo(1);
            assertThat(JsonPath.<Integer> read(changeEvents.get(0).key(), "$.schemaId")).isNotNull();
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.payload.op")).isEqualTo("r");
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.payload.after.title")).isEqualTo("Be Awesome");

            assertThat(JsonPath.<Integer> read(changeEvents.get(1).key(), "$.payload.id")).isEqualTo(2);
            assertThat(JsonPath.<String> read(changeEvents.get(1).value(), "$.payload.op")).isEqualTo("r");
            assertThat(JsonPath.<String> read(changeEvents.get(1).value(), "$.payload.after.title")).isEqualTo("Learn Quarkus");

            statement.execute("update todo.Todo set title = 'Learn Java' where id = 2");

            changeEvents = drain(consumer, 1);

            assertThat(JsonPath.<Integer> read(changeEvents.get(0).key(), "$.payload.id")).isEqualTo(2);
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.payload.op")).isEqualTo("u");
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.payload.before.title")).isEqualTo("Learn Quarkus");
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.payload.after.title")).isEqualTo("Learn Java");

            consumer.unsubscribe();
        }
    }

    @Test
    public void shouldConvertToAvro() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                Statement statement = connection.createStatement();
                KafkaConsumer<byte[], byte[]> consumer = getConsumerBytes(kafkaContainer)) {

            statement.execute("drop schema if exists todo cascade");
            statement.execute("create schema todo");
            statement.execute("create table todo.Todo (id int8 not null, title varchar(255), primary key (id))");
            statement.execute("alter table todo.Todo replica identity full");
            statement.execute("insert into todo.Todo values (1, 'Be Awesome')");

            debeziumContainer.registerConnector("my-connector-avro", getConfiguration(
                    2, "io.apicurio.registry.utils.converter.AvroConverter"));

            consumer.subscribe(Arrays.asList("dbserver2.todo.todo"));

            List<ConsumerRecord<byte[], byte[]>> changeEvents = drain(consumer, 1);

            // Verify magic byte of Avro messages
            assertThat(changeEvents.get(0).key()[0]).isZero();
            assertThat(changeEvents.get(0).value()[0]).isZero();

            consumer.unsubscribe();
        }
    }

    private Connection getConnection(PostgreSQLContainer<?> postgresContainer) throws SQLException {
        return DriverManager.getConnection(postgresContainer.getJdbcUrl(), postgresContainer.getUsername(),
                postgresContainer.getPassword());
    }

    private KafkaConsumer<String, String> getConsumerString(KafkaContainer kafkaContainer) {
        return new KafkaConsumer<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private KafkaConsumer<byte[], byte[]> getConsumerBytes(KafkaContainer kafkaContainer) {
        return new KafkaConsumer<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    }

    private <T> List<ConsumerRecord<T, T>> drain(KafkaConsumer<T, T> consumer, int expectedRecordCount) {
        List<ConsumerRecord<T, T>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(50).toMillis())
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }

    private ConnectorConfiguration getConfiguration(int id, String converter, String... options) {
        final String host = apicurioContainer.getContainerInfo().getConfig().getHostName();
        final int port = apicurioContainer.getExposedPorts().get(0);
        final String apicurioUrl = "http://" + host + ":" + port + "/api";

        // host, database, user etc. are obtained from the container
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(postgresContainer)
                .with("database.server.name", "dbserver" + id)
                .with("slot.name", "debezium_" + id)
                .with("key.converter", converter)
                .with("key.converter.apicurio.registry.url", apicurioUrl)
                .with("key.converter.apicurio.registry.global-id", "io.apicurio.registry.utils.serde.strategy.AutoRegisterIdStrategy")
                .with("value.converter.apicurio.registry.url", apicurioUrl)
                .with("value.converter", converter)
                .with("value.converter.apicurio.registry.global-id", "io.apicurio.registry.utils.serde.strategy.AutoRegisterIdStrategy");

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }
        return config;
    }
}
