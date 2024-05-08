/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.jdbc.sink;

import static io.debezium.testing.system.assertions.JdbcAssertions.awaitAssert;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordBuilder;
import io.debezium.testing.system.assertions.JdbcAssertions;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public abstract class JdbcSinkTests {
    protected final KafkaController kafkaController;
    protected final KafkaConnectController connectController;
    protected final JdbcAssertions assertions;
    protected ConnectorConfigBuilder connectorConfig;
    protected Producer<String, String> kafkaProducer;
    Logger LOGGER = LoggerFactory.getLogger(JdbcAssertions.class);

    public JdbcSinkTests(KafkaController kafkaController,
                         KafkaConnectController connectController,
                         JdbcAssertions assertions,
                         ConnectorConfigBuilder connectorConfig) {
        this.kafkaController = kafkaController;
        this.connectController = connectController;
        this.assertions = assertions;
        this.connectorConfig = connectorConfig;
        this.kafkaProducer = new KafkaProducer<>(kafkaController.getDefaultProducerProperties());
    }

    private void produceRecordToTopic(String topic, String fieldName, String fieldValue) {
        String kafkaRecord = createRecord(fieldName, fieldValue);
        LOGGER.info("Producing record to topic {}", topic);
        LOGGER.debug(kafkaRecord);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, kafkaRecord);
        kafkaProducer.send(producerRecord);
    }

    private String createRecord(String fieldName, String fieldValue) {
        DebeziumSinkRecordFactory factory = new DebeziumSinkRecordFactory();

        SinkRecord record = SinkRecordBuilder.update() // TODO: Change to create when fixed in JDBC connector testsuite
                .flat(false)
                .name("jdbc-connector-test")
                .recordSchema(SchemaBuilder.struct().field(fieldName, Schema.STRING_SCHEMA).build())
                .sourceSchema(factory.basicSourceSchema())
                .after(fieldName, fieldValue)
                .before(fieldName, fieldValue)
                .source("ts_ms", (int) Instant.now().getEpochSecond()).build();
        byte[] recordInBytes;
        try (JsonConverter converter = new JsonConverter()) {
            Map<String, String> config = new HashMap<>();
            config.put("converter.type", "value");
            converter.configure(config);
            recordInBytes = converter.fromConnectData(null, record.valueSchema(), record.value());
        }
        return new String(recordInBytes, StandardCharsets.UTF_8);
    }

    @Test
    @Order(10)
    public void shouldHaveRegisteredConnector() {

        Request r = new Request.Builder().url(connectController.getApiURL().resolve("/connectors")).build();

        awaitAssert(() -> {
            try (Response res = new OkHttpClient().newCall(r).execute()) {
                assertThat(res.body().string()).contains(connectorConfig.getConnectorName());
            }
        });
    }

    @Test
    @Order(20)
    public void shouldStreamChanges() {
        String topic = connectorConfig.getAsString("topics");
        produceRecordToTopic(topic, "name", "Jerry");

        awaitAssert(() -> assertions.assertRowsCount(1, topic));
        awaitAssert(() -> assertions.assertRowsContain(topic, "name", "Jerry"));
    }

    @Test
    @Order(30)
    public void shouldBeDown() throws Exception {
        String topic = connectorConfig.getAsString("topics");
        connectController.undeployConnector(connectorConfig.getConnectorName());
        produceRecordToTopic(topic, "name", "Nibbles");

        awaitAssert(() -> assertions.assertRowsCount(1, topic));
    }

    @Test
    @Order(40)
    public void shouldResumeStreamingAfterRedeployment() throws Exception {
        connectController.deployConnector(connectorConfig);

        String topic = connectorConfig.getAsString("topics");
        awaitAssert(() -> assertions.assertRowsCount(2, topic));
        awaitAssert(() -> assertions.assertRowsContain(topic, "name", "Nibbles"));
    }

    @Test
    @Order(50)
    public void shouldBeDownAfterCrash() {
        connectController.destroy();
        String topic = connectorConfig.getAsString("topics");
        produceRecordToTopic(topic, "name", "Larry");

        awaitAssert(() -> assertions.assertRowsCount(2, topic));
    }

    @Test
    @Order(60)
    public void shouldResumeStreamingAfterCrash() throws InterruptedException {
        connectController.restore();

        String topic = connectorConfig.getAsString("topics");
        awaitAssert(() -> assertions.assertRowsCount(3, topic));
        awaitAssert(() -> assertions.assertRowsContain(topic, "name", "Larry"));
    }
}
