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
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.ReplaceOneModel;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.sink.eventhandler.relational.RelationalEventHandler;
import io.debezium.data.Envelope;
import io.debezium.sink.naming.DefaultColumnNamingStrategy;

class ColumnNamingStrategyTest {

    private static final String CE_PATTERN = ".*CloudEvents\\.Envelope$";

    @Test
    void defaultStrategyShouldPassthroughFieldNames() {
        var strategy = new DefaultColumnNamingStrategy();

        assertThat(strategy.resolveColumnName("user_name")).isEqualTo("user_name");
        assertThat(strategy.resolveColumnName("CamelCase")).isEqualTo("CamelCase");
        assertThat(strategy.resolveColumnName("with$pecial")).isEqualTo("with$pecial");
    }

    @Test
    void columnNamingStrategyShouldBeAppliedInUpsertDoc() {
        var keyDoc = BsonDocument.parse("{\"id\": 1}");
        var afterDoc = BsonDocument.parse("{\"name\": \"John\", \"email\": \"john@example.com\"}");
        var filterDoc = BsonDocument.parse("{\"_id\": {\"id\": 1}}");

        BsonDocument upsertDoc = RelationalEventHandler.generateUpsertOrReplaceDoc(
                keyDoc, afterDoc, filterDoc, new DefaultColumnNamingStrategy());

        assertThat(upsertDoc.containsKey("_id")).isTrue();
        assertThat(upsertDoc.containsKey("name")).isTrue();
        assertThat(upsertDoc.containsKey("email")).isTrue();
    }

    @Test
    void customColumnNamingStrategyShouldTransformFieldNames() {
        var keyDoc = BsonDocument.parse("{\"userId\": 1}");
        var afterDoc = BsonDocument.parse("{\"firstName\": \"John\"}");
        var filterDoc = BsonDocument.parse("{\"_id\": {\"userId\": 1}}");

        BsonDocument upsertDoc = RelationalEventHandler.generateUpsertOrReplaceDoc(
                keyDoc, afterDoc, filterDoc, fieldName -> fieldName.toLowerCase());

        assertThat(upsertDoc.containsKey("_id")).isTrue();
        assertThat(upsertDoc.containsKey("firstname")).isTrue();
    }

    @Test
    void writeModelShouldUseColumnNamingStrategy() {
        var config = new MongoDbSinkConnectorConfig(Configuration.create()
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "testdb")
                .build());

        SinkRecord kafkaRecord = createEnvelopeSinkRecord("server1.schema.table", 1, "John");
        var processed = new MongoDbSinkRecord(kafkaRecord, config);

        assertThat(processed.getWriteModel()).isNotNull();
        assertThat(processed.getWriteModel()).isInstanceOf(ReplaceOneModel.class);
    }

    // --- Helper methods ---

    private static KafkaDebeziumSinkRecord createEnvelopeRecord(String topic, int id, String name) {
        Schema sourceSchema = SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        Struct source = new Struct(sourceSchema).put("ts_ms", Instant.now().toEpochMilli());
        Struct after = new Struct(recordSchema).put("id", id).put("name", name);
        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.AFTER, after)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", id);
        SinkRecord kafkaRecord = new SinkRecord(topic, 0, keySchema, key, envelopeSchema, envelope, 0);
        return new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);
    }

    private static SinkRecord createEnvelopeSinkRecord(String topic, int id, String name) {
        Schema sourceSchema = SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        Struct source = new Struct(sourceSchema).put("ts_ms", Instant.now().toEpochMilli());
        Struct after = new Struct(recordSchema).put("id", id).put("name", name);
        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.AFTER, after)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", id);
        return new SinkRecord(topic, 0, keySchema, key, envelopeSchema, envelope, 0);
    }
}
