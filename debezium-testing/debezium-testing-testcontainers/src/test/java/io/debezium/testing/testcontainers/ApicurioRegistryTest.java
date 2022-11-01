/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import com.jayway.jsonpath.JsonPath;

/**
 * An integration test verifying the Apicurio registry is interoperable with Debezium
 *
 * @author Jiri Pechanec
 */
public class ApicurioRegistryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApicurioRegistryTest.class);

    private static Network network = Network.newNetwork();

    private static final ApicurioRegistryContainer apicurioContainer = new ApicurioRegistryContainer().withNetwork(network);

    private static KafkaContainer kafkaContainer = new KafkaContainer()
            .withNetwork(network);

    public static PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(ImageNames.POSTGRES_DOCKER_IMAGE_NAME)
            .withNetwork(network)
            .withNetworkAliases("postgres");

    public static DebeziumContainer debeziumContainer = DebeziumContainer.nightly()
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .enableApicurioConverters()
            .dependsOn(kafkaContainer);

    @BeforeAll
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
                    2, "io.apicurio.registry.utils.converter.AvroConverter",
                    "schema.name.adjustment.mode", "avro"));

            consumer.subscribe(Arrays.asList("dbserver2.todo.todo"));

            List<ConsumerRecord<byte[], byte[]>> changeEvents = drain(consumer, 1);

            // Verify magic byte of Avro messages
            assertThat(changeEvents.get(0).key()[0]).isZero();
            assertThat(changeEvents.get(0).value()[0]).isZero();

            consumer.unsubscribe();
        }
    }

    @Test
    public void shouldConvertToCloudEventWithDataAsAvro() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                Statement statement = connection.createStatement();
                KafkaConsumer<String, String> consumer = getConsumerString(kafkaContainer)) {

            statement.execute("drop schema if exists todo cascade");
            statement.execute("create schema todo");
            statement.execute("create table todo.Todo (id int8 not null, title varchar(255), primary key (id))");
            statement.execute("alter table todo.Todo replica identity full");
            statement.execute("insert into todo.Todo values (3, 'Be Awesome')");

            final String apicurioUrl = getApicurioUrl();
            String id = "3";

            // host, database, user etc. are obtained from the container
            final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(postgresContainer)
                    .with("topic.prefix", "dbserver" + id)
                    .with("slot.name", "debezium_" + id)
                    .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                    .with("value.converter", "io.debezium.converters.CloudEventsConverter")
                    .with("value.converter.data.serializer.type", "avro")
                    .with("value.converter.avro.apicurio.registry.url", apicurioUrl)
                    .with("value.converter.avro.apicurio.registry.auto-register", "true")
                    .with("value.converter.avro.apicurio.registry.find-latest", "true");

            debeziumContainer.registerConnector("my-connector-cloudevents-avro", config);

            consumer.subscribe(Arrays.asList("dbserver3.todo.todo"));

            List<ConsumerRecord<String, String>> changeEvents = drain(consumer, 1);

            assertThat(JsonPath.<Integer> read(changeEvents.get(0).key(), "$.payload.id")).isEqualTo(3);
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.iodebeziumop")).isEqualTo("r");
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.iodebeziumname")).isEqualTo("dbserver3");
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.datacontenttype")).isEqualTo("application/avro");
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.iodebeziumtable")).isEqualTo("todo");

            // Verify magic byte of Avro messages
            byte[] decodedBytes = Base64.getDecoder().decode(JsonPath.<String> read(changeEvents.get(0).value(), "$.data"));
            assertThat(decodedBytes[0]).isZero();

            consumer.unsubscribe();
        }
    }

    private Connection getConnection(PostgreSQLContainer<?> postgresContainer) throws SQLException {
        return DriverManager.getConnection(postgresContainer.getJdbcUrl(), postgresContainer.getUsername(),
                postgresContainer.getPassword());
    }

    private KafkaConsumer<String, String> getConsumerString(KafkaContainer kafkaContainer) {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private KafkaConsumer<byte[], byte[]> getConsumerBytes(KafkaContainer kafkaContainer) {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    }

    private <T> List<ConsumerRecord<T, T>> drain(KafkaConsumer<T, T> consumer, int expectedRecordCount) {
        List<ConsumerRecord<T, T>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(50))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }

    private ConnectorConfiguration getConfiguration(int id, String converter, String... options) {
        final String apicurioUrl = getApicurioUrl();

        // host, database, user etc. are obtained from the container
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(postgresContainer)
                .with("topic.prefix", "dbserver" + id)
                .with("slot.name", "debezium_" + id)
                .with("key.converter", converter)
                .with("key.converter.apicurio.registry.url", apicurioUrl)
                .with("key.converter.apicurio.registry.auto-register", "true")
                .with("key.converter.apicurio.registry.find-latest", "true")
                .with("value.converter.apicurio.registry.url", apicurioUrl)
                .with("value.converter", converter)
                .with("value.converter.apicurio.registry.auto-register", "true")
                .with("value.converter.apicurio.registry.find-latest", "true");

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }
        return config;
    }

    private String getApicurioUrl() {
        final String host = apicurioContainer.getContainerInfo().getConfig().getHostName();
        final int port = apicurioContainer.getExposedPorts().get(0);
        final String apicurioUrl = "http://" + host + ":" + port + "/apis/registry/v2";
        return apicurioUrl;
    }

    @AfterAll
    public static void stopContainers() {
        try {
            if (postgresContainer != null) {
                postgresContainer.stop();
            }
            if (apicurioContainer != null) {
                apicurioContainer.stop();
            }
            if (kafkaContainer != null) {
                kafkaContainer.stop();
            }
            if (debeziumContainer != null) {
                debeziumContainer.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }
}
