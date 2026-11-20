/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.util.DockerUtils;

class MongoDbSinkConnectorTaskIT {

    private static final String DATABASE_NAME = "inventory";
    private static final String TOPIC = "dbserver1.inventory.hotfix";

    private static MongoDbReplicaSet mongo;

    @BeforeAll
    static void beforeAll() {
        DockerUtils.enableFakeDnsIfRequired();
        mongo = MongoDbReplicaSet.replicaSet()
                .memberCount(1)
                .namespace("sink-hotfix-mongo")
                .build();
        mongo.start();
    }

    @AfterEach
    void afterEach() {
        TestHelper.cleanDatabase(mongo, DATABASE_NAME);
    }

    @AfterAll
    static void afterAll() {
        if (mongo != null) {
            mongo.stop();
        }
        DockerUtils.disableFakeDns();
    }

    @FixFor("debezium/dbz#2469")
    @Test
    void shouldDeleteRecordWhenOpenLineageIsDisabled() {
        final var config = sinkConfig();
        final var deleteRecord = envelopeRecord(Envelope.Operation.DELETE, 77, "before", 1);

        try (var client = TestHelper.connect(mongo)) {
            final var collection = client.getDatabase(DATABASE_NAME).getCollection(collectionName(), BsonDocument.class);
            collection.insertOne(new BsonDocument("_id", new BsonDocument("id", new BsonInt32(77)))
                    .append("name", new BsonString("before")));

            final var sink = new MongoDbChangeEventSink(config, client, MongoDbSinkConnectorTask.nopErrorReporter(), connectorContext());
            sink.execute(List.of(deleteRecord));

            assertThat(collection.countDocuments()).isZero();
        }
    }

    @FixFor("debezium/dbz#2469")
    @Test
    void shouldSkipTombstoneWithoutDroppingOtherRecordsInBatch() {
        final var config = sinkConfig();
        final var firstRecord = envelopeRecord(Envelope.Operation.CREATE, 1, "first", 1);
        final var tombstone = tombstoneRecord(2);
        final var secondRecord = envelopeRecord(Envelope.Operation.CREATE, 3, "third", 3);

        try (var client = TestHelper.connect(mongo)) {
            final var sink = new MongoDbChangeEventSink(config, client, MongoDbSinkConnectorTask.nopErrorReporter(), connectorContext());
            sink.execute(List.of(firstRecord, tombstone, secondRecord));

            final var collection = client.getDatabase(DATABASE_NAME).getCollection(collectionName(), BsonDocument.class);
            assertThat(collection.countDocuments()).isEqualTo(2);
        }
    }

    @FixFor("debezium/dbz#2469")
    @Test
    void shouldPropagateRecordProcessingFailureFromTaskPut() {
        final var task = new MongoDbSinkConnectorTask();
        task.start(sinkConfiguration().asMap());
        try {
            final var invalidValueSchema = SchemaBuilder.struct()
                    .name("invalid.CloudEvents.Envelope")
                    .build();
            final var invalidRecord = new SinkRecord(TOPIC, 0, null, null, invalidValueSchema, "not-a-cloud-event", 1);

            assertThatThrownBy(() -> task.put(List.of(invalidRecord)))
                    .isInstanceOf(DebeziumException.class);
        }
        finally {
            task.stop();
        }
    }

    private MongoDbSinkConnectorConfig sinkConfig() {
        return new MongoDbSinkConnectorConfig(sinkConfiguration());
    }

    private Configuration sinkConfiguration() {
        return Configuration.create()
                .with("name", "mongodb-sink-hotfix-test")
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, mongo.getConnectionString())
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, DATABASE_NAME)
                .build();
    }

    private ConnectorContext connectorContext() {
        return ConnectorContext.from(sinkConfiguration().asMap(), Module.name(), UUID.randomUUID());
    }

    private String collectionName() {
        return TOPIC.replace('.', '_');
    }

    private SinkRecord tombstoneRecord(long offset) {
        final var keySchema = keySchema();
        final var key = new Struct(keySchema).put("id", 2);
        return new SinkRecord(TOPIC, 0, keySchema, key, null, null, offset);
    }

    private SinkRecord envelopeRecord(Envelope.Operation operation, int id, String name, long offset) {
        final var recordSchema = recordSchema();
        final var sourceSchema = SchemaBuilder.struct()
                .field(Envelope.FieldName.TIMESTAMP, Schema.INT64_SCHEMA)
                .build();
        final var envelopeSchema = SchemaBuilder.struct()
                .name(TOPIC + ".Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        final var keySchema = keySchema();
        final var key = new Struct(keySchema).put("id", id);
        final var row = new Struct(recordSchema).put("id", id).put("name", name);
        final var envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.SOURCE, new Struct(sourceSchema).put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli()))
                .put(Envelope.FieldName.OPERATION, operation.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());

        if (Envelope.Operation.DELETE.equals(operation)) {
            envelope.put(Envelope.FieldName.BEFORE, row);
        }
        else {
            envelope.put(Envelope.FieldName.AFTER, row);
        }

        return new SinkRecord(TOPIC, 0, keySchema, key, envelopeSchema, envelope, offset);
    }

    private Schema keySchema() {
        return SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
    }

    private Schema recordSchema() {
        return SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }
}
