/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static io.debezium.junit.EqualityCheck.GREATER_THAN_OR_EQUAL;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static io.debezium.junit.SkipWhenKafkaVersion.KafkaVersion.KAFKA_241;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.util.Collect;

/**
 * Unit test for {@link ExtractNewDocumentState}.
 *
 * @author Gunnar Morling
 */
public class ExtractNewDocumentStateTest {

    protected ExtractNewDocumentState<SourceRecord> transformation;

    @Rule
    public TestRule skipTestRule = new SkipTestRule();

    @Before
    public void setup() {
        transformation = new ExtractNewDocumentState<>();
        transformation.configure(Collect.hashMapOf(
                "array.encoding", "array",
                "delete.tombstone.handling.mode", "tombstone"));
    }

    @After
    public void closeSmt() {
        transformation.close();
    }

    @Test
    @FixFor("DBZ-1430")
    public void shouldPassHeartbeatMessages() {
        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Heartbeat")
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema).put(AbstractSourceInfo.TIMESTAMP_KEY, 1565787098802L);

        Schema keySchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.ServerNameKey")
                .field("serverName", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("serverName", "op.with.heartbeat");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "op.with.heartbeat",
                keySchema,
                key,
                valueSchema,
                value);

        // when
        SourceRecord transformed = transformation.apply(eventRecord);

        assertThat(transformed).isSameAs(eventRecord);
    }

    @Test
    @FixFor("DBZ-1430")
    public void shouldSkipMessagesWithoutDebeziumCdcEnvelopeDueToMissingSchemaName() {
        Schema valueSchema = SchemaBuilder.struct()
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);

        Schema keySchema = SchemaBuilder.struct()
                .name("op.with.heartbeat.Key")
                .field("id", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("id", "123");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "op.with.heartbeat",
                keySchema,
                key,
                valueSchema,
                value);

        // when
        SourceRecord transformed = transformation.apply(eventRecord);

        assertThat(transformed).isSameAs(eventRecord);
    }

    @Test
    @FixFor("DBZ-1430")
    public void shouldSkipMessagesWithoutDebeziumCdcEnvelopeDueToMissingSchemaNameSuffix() {
        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Heartbeat")
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);

        Schema keySchema = SchemaBuilder.struct()
                .name("op.with.heartbeat.Key")
                .field("id", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("id", "123");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "op.with.heartbeat",
                keySchema,
                key,
                valueSchema,
                value);

        // when
        SourceRecord transformed = transformation.apply(eventRecord);

        assertThat(transformed).isSameAs(eventRecord);
    }

    @Test
    @FixFor("DBZ-1430")
    public void shouldSkipMessagesWithoutDebeziumCdcEnvelopeDueToMissingValueSchema() {
        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Heartbeat.Envelope")
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);

        Schema keySchema = SchemaBuilder.struct()
                .name("op.with.heartbeat.Key")
                .field("id", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("id", "123");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "op.with.heartbeat",
                keySchema,
                key,
                null,
                value);

        // when
        SourceRecord transformed = transformation.apply(eventRecord);

        assertThat(transformed).isSameAs(eventRecord);
    }

    @Test
    @FixFor("DBZ-1430")
    @SkipWhenKafkaVersion(check = GREATER_THAN_OR_EQUAL, value = KAFKA_241, description = "Kafka throws IllegalArgumentException after 2.4.1")
    public void shouldFailWhenTheSchemaLooksValidButDoesNotHaveTheCorrectFieldsPreKafka241() {
        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Heartbeat.Envelope")
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);

        Schema keySchema = SchemaBuilder.struct()
                .name("op.with.heartbeat.Key")
                .field("id", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("id", "123");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "op.with.heartbeat",
                keySchema,
                key,
                valueSchema,
                value);

        // when
        assertThrows(NullPointerException.class, () -> transformation.apply(eventRecord));
    }

    @Test
    @FixFor("DBZ-1430")
    @SkipWhenKafkaVersion(check = LESS_THAN, value = KAFKA_241, description = "Kafka throws NullPointerException prior to 2.4.1")
    public void shouldFailWhenTheSchemaLooksValidButDoesNotHaveTheCorrectFieldsPostKafka241() {
        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Heartbeat.Envelope")
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);

        Schema keySchema = SchemaBuilder.struct()
                .name("op.with.heartbeat.Key")
                .field("id", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("id", "123");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "op.with.heartbeat",
                keySchema,
                key,
                valueSchema,
                value);

        // when
        assertThrows(IllegalArgumentException.class, () -> transformation.apply(eventRecord));
    }
}
