/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

@Disabled
public class DebeziumContainerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumContainerTest.class);

    private static final Network network = Network.newNetwork();

    private static final KafkaContainer kafkaContainer = new KafkaContainer()
            .withNetwork(network);

    public static PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(ImageNames.POSTGRES_DOCKER_IMAGE_NAME)
            .withNetwork(network)
            .withNetworkAliases("postgres");

    public static DebeziumContainer debeziumContainer = DebeziumContainer.nightly()
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .dependsOn(kafkaContainer);

    @BeforeAll
    public static void startContainers() {
        Startables.deepStart(Stream.of(
                kafkaContainer, postgresContainer, debeziumContainer)).join();
    }

    @Test
    public void canRegisterConnector() throws Exception {
        debeziumContainer.registerConnector("my-connector-1", getConfiguration(1));

        // task initialization happens asynchronously, so we might have to retry until the task is RUNNING
        Awaitility.await()
                .pollInterval(Duration.ofMillis(250))
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(
                        () -> {
                            String status = executeHttpRequest(debeziumContainer.getConnectorStatusUri("my-connector-1"));

                            assertThat(JsonPath.<String> read(status, "$.name")).isEqualTo("my-connector-1");
                            assertThat(JsonPath.<String> read(status, "$.connector.state")).isEqualTo("RUNNING");
                            assertThat(JsonPath.<String> read(status, "$.tasks[0].state")).isEqualTo("RUNNING");
                        });
    }

    @Test
    public void shouldRegisterPostgreSQLConnector() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                Statement statement = connection.createStatement();
                KafkaConsumer<String, String> consumer = getConsumer(kafkaContainer)) {

            statement.execute("create schema todo");
            statement.execute("create table todo.Todo (id int8 not null, title varchar(255), primary key (id))");
            statement.execute("alter table todo.Todo replica identity full");
            statement.execute("insert into todo.Todo values (1, 'Be Awesome')");
            statement.execute("insert into todo.Todo values (2, 'Learn Quarkus')");

            debeziumContainer.registerConnector("my-connector", getConfiguration(2));

            consumer.subscribe(Arrays.asList("dbserver2.todo.todo"));

            List<ConsumerRecord<String, String>> changeEvents = drain(consumer, 2);

            assertThat(JsonPath.<Integer> read(changeEvents.get(0).key(), "$.id")).isEqualTo(1);
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.op")).isEqualTo("r");
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.after.title")).isEqualTo("Be Awesome");

            assertThat(JsonPath.<Integer> read(changeEvents.get(1).key(), "$.id")).isEqualTo(2);
            assertThat(JsonPath.<String> read(changeEvents.get(1).value(), "$.op")).isEqualTo("r");
            assertThat(JsonPath.<String> read(changeEvents.get(1).value(), "$.after.title")).isEqualTo("Learn Quarkus");

            statement.execute("update todo.Todo set title = 'Learn Java' where id = 2");

            changeEvents = drain(consumer, 1);

            assertThat(JsonPath.<Integer> read(changeEvents.get(0).key(), "$.id")).isEqualTo(2);
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.op")).isEqualTo("u");
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.before.title")).isEqualTo("Learn Quarkus");
            assertThat(JsonPath.<String> read(changeEvents.get(0).value(), "$.after.title")).isEqualTo("Learn Java");

            consumer.unsubscribe();
        }
    }

    private Connection getConnection(PostgreSQLContainer<?> postgresContainer) throws SQLException {
        return DriverManager.getConnection(postgresContainer.getJdbcUrl(), postgresContainer.getUsername(),
                postgresContainer.getPassword());
    }

    private KafkaConsumer<String, String> getConsumer(KafkaContainer kafkaContainer) {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private List<ConsumerRecord<String, String>> drain(KafkaConsumer<String, String> consumer, int expectedRecordCount) {
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(50))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }

    private ConnectorConfiguration getConfiguration(int id) {
        // host, database, user etc. are obtained from the container
        return ConnectorConfiguration.forJdbcContainer(postgresContainer)
                .with("topic.prefix", "dbserver" + id)
                .with("slot.name", "debezium_" + id);
    }

    private String executeHttpRequest(String url) throws IOException {
        final OkHttpClient client = new OkHttpClient();
        final Request request = new Request.Builder().url(url).build();

        try (Response response = client.newCall(request).execute()) {
            return response.body().string();
        }
    }

    @AfterAll
    public static void stopContainers() {
        try {
            if (postgresContainer != null) {
                postgresContainer.stop();
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
