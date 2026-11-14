/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;
import io.debezium.sink.naming.PassthroughCollectionNamingStrategy;

class CollectionNamingStrategyTest {

    private static final String CE_PATTERN = ".*CloudEvents\\.Envelope$";

    @Test
    void defaultStrategyShouldReplaceDotsWithUnderscores() {
        var strategy = new DefaultCollectionNamingStrategy();
        var record = createFlatRecord("server1.inventory.customers");

        String name = strategy.resolveCollectionName(record, "${topic}");
        assertThat(name).isEqualTo("server1_inventory_customers");
    }

    @Test
    void defaultStrategyShouldApplyCollectionNameFormat() {
        var strategy = new DefaultCollectionNamingStrategy();
        var record = createFlatRecord("server1.inventory.customers");

        String name = strategy.resolveCollectionName(record, "prefix_${topic}");
        assertThat(name).isEqualTo("prefix_server1_inventory_customers");
    }

    @Test
    void defaultStrategyShouldResolveSourceFields() {
        var strategy = new DefaultCollectionNamingStrategy();
        var record = createEnvelopeRecord("server1.inventory.customers", "mydb", "mytable");

        String name = strategy.resolveCollectionName(record, "${source.db}.${source.table}");
        assertThat(name).isEqualTo("mydb.mytable");
    }

    @Test
    void defaultStrategyShouldReturnNullForSourceFieldOnNonDebeziumRecord() {
        var strategy = new DefaultCollectionNamingStrategy();
        var record = createFlatRecord("server1.inventory.customers");

        String name = strategy.resolveCollectionName(record, "${source.db}");
        assertThat(name).isNull();
    }

    @Test
    void passthroughStrategyShouldPreserveDots() {
        var strategy = new PassthroughCollectionNamingStrategy();
        var record = createFlatRecord("server1.inventory.customers");

        String name = strategy.resolveCollectionName(record, "${topic}");
        assertThat(name).isEqualTo("server1.inventory.customers");
    }

    @Test
    void configuredCollectionNameFormatShouldBeUsed() {
        var config = new MongoDbSinkConnectorConfig(Configuration.create()
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "testdb")
                .with(MongoDbSinkConnectorConfig.COLLECTION_NAME_FORMAT, "sink_${topic}")
                .build());
        var strategy = config.getCollectionNamingStrategy();
        var record = createFlatRecord("server1.inventory.products");

        String name = strategy.resolveCollectionName(record, config.getCollectionNameFormat());
        assertThat(name).isEqualTo("sink_server1_inventory_products");
    }

    @Test
    void namespaceResolutionViaCollectionNamingStrategy() {
        var config = new MongoDbSinkConnectorConfig(Configuration.create()
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "testdb")
                .with(MongoDbSinkConnectorConfig.COLLECTION_NAME_FORMAT, "${topic}")
                .build());
        var record = createFlatRecord("server1.inventory.orders");
        String collectionName = config.getCollectionNamingStrategy().resolveCollectionName(record, config.getCollectionNameFormat());

        assertThat(collectionName).isEqualTo("server1_inventory_orders");
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
        return new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);
    }

    private static KafkaDebeziumSinkRecord createEnvelopeRecord(String topic, String db, String table) {
        Schema sourceSchema = SchemaBuilder.struct()
                .field("db", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();
        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        Struct source = new Struct(sourceSchema)
                .put("db", db).put("table", table).put("ts_ms", Instant.now().toEpochMilli());
        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.AFTER, new Struct(recordSchema).put("id", 1))
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", 1);
        SinkRecord kafkaRecord = new SinkRecord(topic, 0, keySchema, key, envelopeSchema, envelope, 0);
        return new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);
    }
}
