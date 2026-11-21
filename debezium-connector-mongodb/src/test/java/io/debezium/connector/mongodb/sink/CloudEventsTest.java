/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.config.Configuration;

class CloudEventsTest {

    private static final String TOPIC = "server1.inventory.customers";

    @Test
    void shouldDetectCloudEventsRecordByDefaultPattern() {
        KafkaDebeziumSinkRecord record = createCloudEventsRecord(TOPIC, "io.debezium.connector.mysql.CloudEvents.Envelope");

        assertThat(record.isSchemaChange()).isFalse();
        Object value = record.value();
        assertThat(value).isNotNull();
        assertThat(value).isInstanceOf(Struct.class);
        Struct dataStruct = (Struct) value;
        assertThat(dataStruct.get("id")).isEqualTo(1);
    }

    @Test
    void shouldDetectCloudEventsRecordByCustomPattern() {
        String customPattern = ".*MyCustomCloudEvents$";
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(
                createCloudEventsSinkRecord(TOPIC, "io.debezium.MyCustomCloudEvents"),
                customPattern);

        Object value = record.value();
        assertThat(value).isNotNull();
        assertThat(value).isInstanceOf(Struct.class);
    }

    @Test
    void shouldNotTreatNormalRecordAsCloudEvents() {
        KafkaDebeziumSinkRecord record = createFlatRecord(TOPIC);

        assertThat(record.isFlattened()).isTrue();
        assertThat(record.isDebeziumMessage()).isFalse();
    }

    @Test
    void cloudEventsPatternShouldBeConfigurable() {
        var config = new MongoDbSinkConnectorConfig(Configuration.create()
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "testdb")
                .with("cloud.events.schema.name.pattern", ".*MyCE$")
                .build());

        assertThat(config.cloudEventsSchemaNamePattern()).isEqualTo(".*MyCE$");
    }

    @Test
    void shouldResolveNamespaceForCloudEventsRecord() {
        var config = new MongoDbSinkConnectorConfig(Configuration.create()
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "testdb")
                .build());

        var processed = new MongoDbSinkRecord(createCloudEventsSinkRecord(TOPIC, "io.debezium.connector.mysql.CloudEvents.Envelope"), config);
        String collectionName = config.getCollectionNamingStrategy().resolveCollectionName(processed, config.getCollectionNameFormat());

        assertThat(collectionName).isEqualTo("server1_inventory_customers");
        assertThat(config.getSinkDatabaseName()).isEqualTo("testdb");
    }

    // --- Helper methods ---

    private static KafkaDebeziumSinkRecord createFlatRecord(String topic) {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct value = new Struct(schema).put("id", 1).put("name", "test");
        SinkRecord kafkaRecord = new SinkRecord(topic, 0, Schema.INT32_SCHEMA, 1, schema, value, 0);
        return new KafkaDebeziumSinkRecord(kafkaRecord, ".*CloudEvents\\.Envelope$");
    }

    private static KafkaDebeziumSinkRecord createCloudEventsRecord(String topic, String ceSchemaName) {
        SinkRecord kafkaRecord = createCloudEventsSinkRecord(topic, ceSchemaName);
        return new KafkaDebeziumSinkRecord(kafkaRecord, ".*CloudEvents\\.Envelope$");
    }

    private static SinkRecord createCloudEventsSinkRecord(String topic, String ceSchemaName) {
        Schema dataSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema ceSchema = SchemaBuilder.struct()
                .name(ceSchemaName)
                .field("id", Schema.STRING_SCHEMA)
                .field("source", Schema.STRING_SCHEMA)
                .field("specversion", Schema.STRING_SCHEMA)
                .field("type", Schema.STRING_SCHEMA)
                .field("data", dataSchema)
                .build();
        Struct data = new Struct(dataSchema).put("id", 1).put("name", "John");
        Struct ceValue = new Struct(ceSchema)
                .put("id", "event-1")
                .put("source", "/debezium/mysql")
                .put("specversion", "1.0")
                .put("type", "io.debezium.connector.mysql.DataChangeEvent")
                .put("data", data);

        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", 1);
        return new SinkRecord(topic, 0, keySchema, key, ceSchema, ceValue, 0);
    }
}
