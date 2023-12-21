/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static io.debezium.junit.EqualityCheck.GREATER_THAN_OR_EQUAL;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static io.debezium.junit.SkipWhenKafkaVersion.KafkaVersion.KAFKA_241;
import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.Configurator;
import io.debezium.connector.mongodb.Filters;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.MongoDbTopicSelector;
import io.debezium.connector.mongodb.SourceInfo;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.schema.TopicSelector;

/**
 * Unit test for {@link ExtractNewDocumentState}.
 *
 * @author Gunnar Morling
 */
public class ExtractNewDocumentStateTest {

    private static final String SERVER_NAME = "serverX";

    private Filters filters;
    private SourceInfo source;
    private TopicSelector<CollectionId> topicSelector;
    private List<SourceRecord> produced;

    private ExtractNewDocumentState<SourceRecord> transformation;

    @Rule
    public TestRule skipTestRule = new SkipTestRule();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        filters = new Configurator().createFilters();
        Configuration config = Configuration.create()
                .with(MongoDbConnectorConfig.LOGICAL_NAME, SERVER_NAME)
                .build();
        source = new SourceInfo(new MongoDbConnectorConfig(config), new MongoDbTaskContext(config).getMongoTaskId());
        topicSelector = MongoDbTopicSelector.defaultSelector(SERVER_NAME, "__debezium-heartbeat");
        produced = new ArrayList<>();

        transformation = new ExtractNewDocumentState<SourceRecord>();
        transformation.configure(Collections.singletonMap("array.encoding", "array"));
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

        exceptionRule.expect(NullPointerException.class);

        // when
        SourceRecord transformed = transformation.apply(eventRecord);

        assertThat(transformed).isNull();
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

        exceptionRule.expect(IllegalArgumentException.class);

        // when
        SourceRecord transformed = transformation.apply(eventRecord);

        assertThat(transformed).isNull();
    }
}
