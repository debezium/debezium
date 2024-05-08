/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms.outbox;

import static io.debezium.connector.mongodb.MongoDbSchema.UPDATED_DESCRIPTION_SCHEMA;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.VerifyRecord;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.transforms.outbox.EventRouterConfigDefinition;

/**
 * Unit tests for {@link MongoEventRouter}
 *
 * @author Sungho Hwang
 */
public class MongoEventRouterTest {

    JsonWriterSettings COMPACT_JSON_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.EXTENDED)
            .indent(true)
            .newLineCharacters("\n")
            .build();

    MongoEventRouter<SourceRecord> router;

    @Before
    public void beforeEach() {
        this.router = new MongoEventRouter<>();
    }

    @Test
    public void canSkipTombstone() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.outbox",
                SchemaBuilder.STRING_SCHEMA,
                "123123",
                null,
                null);
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNull();
    }

    @Test
    public void canSkipDeletion() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(Schema.STRING_SCHEMA)
                .withSource(SchemaBuilder.struct().build())
                .build();
        final Struct payload = envelope.delete("{\"_id\": {\"$oid\": \"da8d6de63b7745ff8f4457db\"}}", null, Instant.now());
        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.outbox",
                SchemaBuilder.STRING_SCHEMA,
                "da8d6de63b7745ff8f4457db",
                envelope.schema(),
                payload);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNull();
    }

    @Test
    public void canSkipMessagesWithoutDebeziumCdcEnvelopeDueToMissingSchemaName() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        Schema valueSchema = SchemaBuilder.struct()
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);
        value.put(AbstractSourceInfo.TIMESTAMP_KEY, Instant.now().toEpochMilli());

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.outbox",
                SchemaBuilder.STRING_SCHEMA,
                "772590bf-ef2d-4814-b4bf-ddc6f5f8b9c5",
                valueSchema,
                value);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isSameAs(eventRecord);
    }

    @Test
    public void shouldFailWhenTheSchemaLooksValidButDoesNotHaveTheCorrectFields() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Heartbeat.Envelope")
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);
        value.put(AbstractSourceInfo.TIMESTAMP_KEY, Instant.now().toEpochMilli());

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.outbox",
                SchemaBuilder.STRING_SCHEMA,
                "772590bf-ef2d-4814-b4bf-ddc6f5f8b9c5",
                valueSchema,
                value);

        DataException e = Assert.assertThrows(DataException.class, () -> router.apply(eventRecord));
        assertThat(e).hasMessage("op is not a valid field name");
    }

    @Test
    public void canSkipMessagesWithoutDebeziumCdcEnvelopeDueToMissingSchemaNameSuffix() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Heartbeat")
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);
        value.put(AbstractSourceInfo.TIMESTAMP_KEY, Instant.now().toEpochMilli());

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.outbox",
                SchemaBuilder.STRING_SCHEMA,
                "772590bf-ef2d-4814-b4bf-ddc6f5f8b9c5",
                valueSchema,
                value);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isSameAs(eventRecord);
    }

    @Test
    public void canSkipMessagesWithoutDebeziumCdcEnvelopeDueToMissingValueSchema() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Heartbeat")
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);
        value.put(AbstractSourceInfo.TIMESTAMP_KEY, Instant.now().toEpochMilli());

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.outbox",
                SchemaBuilder.STRING_SCHEMA,
                "772590bf-ef2d-4814-b4bf-ddc6f5f8b9c5",
                null,
                value);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isSameAs(eventRecord);
    }

    @Test
    public void canSkipUpdates() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final Schema recordSchema = SchemaBuilder.struct().field("id", SchemaBuilder.string()).build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", "772590bf-ef2d-4814-b4bf-ddc6f5f8b9c5");
        final Struct payload = envelope.update(before, before, null, Instant.now());
        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.outbox",
                SchemaBuilder.STRING_SCHEMA,
                "772590bf-ef2d-4814-b4bf-ddc6f5f8b9c5",
                envelope.schema(),
                payload);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNull();
    }

    @Test(expected = IllegalStateException.class)
    public void canFailOnUpdates() {
        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR.name(),
                EventRouterConfigDefinition.InvalidOperationBehavior.FATAL.getValue());
        router.configure(config);

        final Schema recordSchema = SchemaBuilder.struct().field("id", SchemaBuilder.string()).build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", "772590bf-ef2d-4814-b4bf-ddc6f5f8b9c5");
        final Struct payload = envelope.update(before, before, null, Instant.now());
        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.outbox",
                SchemaBuilder.STRING_SCHEMA,
                "772590bf-ef2d-4814-b4bf-ddc6f5f8b9c5",
                envelope.schema(),
                payload);

        router.apply(eventRecord);
    }

    @Test
    public void canExtractTableFields() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.value()).isEqualTo("{}");

        assertThat(eventRouted.valueSchema().version()).isNull();
    }

    @Test
    public void canSetDefaultMessageKey() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(eventRouted.key()).isEqualTo("000000000000000000000001");
    }

    @Test
    public void canSetMessageKey() {
        final Map<String, String> config = new HashMap<>();

        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "customField");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de63b7745ff8f4457db",
                "eventType",
                "payloadId",
                "payloadType",
                new Document(),
                Collections.singletonMap("customField", "dummy"));
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(eventRouted.key()).isEqualTo("dummy");
    }

    @Test(expected = DataException.class)
    public void failsOnInvalidSetMessageKey() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "fakefield");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        router.apply(eventRecord);
    }

    @Test
    public void canSetTimestampFromDebeziumEnvelopeByDefault() {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord userEventRecord = createEventRecord();
        final SourceRecord userEventRouted = router.apply(userEventRecord);

        Struct userEvent = requireStruct(userEventRecord.value(), "Test timestamp");
        Long expectedTimestamp = userEvent.getInt64("ts_ms");

        assertThat(userEventRecord.timestamp()).isNull();
        assertThat(userEventRouted.timestamp()).isEqualTo(expectedTimestamp);
    }

    @Test
    public void canSetTimestampByUserDefinedConfiguration() {
        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), "event_timestamp");
        router.configure(config);

        Long expectedTimestamp = 14222264625338L;

        Map<String, Object> extraValues = new HashMap<>();
        extraValues.put("event_timestamp", expectedTimestamp);

        final SourceRecord userEventRecord = createEventRecord(
                "da8d6de63b7745ff8f4457db",
                "UserCreated",
                "420b186d",
                "User",
                new Document(),
                extraValues);
        final SourceRecord userEventRouted = router.apply(userEventRecord);

        assertThat(userEventRecord.timestamp()).isNull();
        assertThat(userEventRouted.timestamp()).isEqualTo(expectedTimestamp);
    }

    @Test
    public void canRouteBasedOnField() {
        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.ROUTE_BY_FIELD.name(),
                "aggregatetype");
        router.configure(config);

        final SourceRecord userEventRecord = createEventRecord();
        final SourceRecord userEventRouted = router.apply(userEventRecord);

        assertThat(userEventRouted).isNotNull();
        assertThat(userEventRouted.topic()).isEqualTo("outbox.event.User");

        final SourceRecord userUpdatedEventRecord = createEventRecord(
                "da8d6de63b7745ff8f4457db",
                "UserUpdate",
                new ObjectId("000000000000000000000001"),
                "User",
                new Document());
        final SourceRecord userUpdatedEventRouted = router.apply(userUpdatedEventRecord);

        assertThat(userUpdatedEventRouted).isNotNull();
        assertThat(userUpdatedEventRouted.topic()).isEqualTo("outbox.event.User");

        final SourceRecord addressCreatedEventRecord = createEventRecord(
                "1a8d6de63b7745ff8f4451db",
                "AddressCreated",
                new ObjectId("000000000000000000000001"),
                "Address",
                new Document());
        final SourceRecord addressCreatedEventRouted = router.apply(addressCreatedEventRecord);

        assertThat(addressCreatedEventRouted).isNotNull();
        assertThat(addressCreatedEventRouted.topic()).isEqualTo("outbox.event.Address");
    }

    @Test
    public void canConfigureEveryTableField() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_ID.name(), "event_id");
        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "payload_id");
        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_TYPE.name(), "event_type");
        config.put(MongoEventRouterConfigDefinition.FIELD_PAYLOAD.name(), "payload_body");
        config.put(MongoEventRouterConfigDefinition.ROUTE_BY_FIELD.name(), "payload_id");
        router.configure(config);

        Document outboxEvent = new Document()
                .append("event_id", new ObjectId("1a8d6de63b7745ff8f4451db"))
                .append("payload_id", "10711fa5")
                .append("event_type", "UserCreated")
                .append("payload_body", new Document());

        String after = outboxEvent.toJson(COMPACT_JSON_SETTINGS);

        final Schema valueSchema = SchemaBuilder.struct()
                .name("event.Envelope")
                .field(Envelope.FieldName.AFTER, Json.builder().optional().build())
                // Change Streams field
                .field(MongoDbFieldName.UPDATE_DESCRIPTION, UPDATED_DESCRIPTION_SCHEMA)
                // .field(Envelope.FieldName.SOURCE, SchemaBuilder.struct().build())
                .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP_US, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP_NS, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                .build();

        final Envelope envelope = Envelope.fromSchema(valueSchema);

        final Struct body = envelope.create(after, null, Instant.now());
        final SourceRecord eventRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), body);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();

        Headers headers = eventRouted.headers();
        assertThat(headers.size()).isEqualTo(1);
        Header header = headers.iterator().next();
        assertThat(header.key()).isEqualTo("id");
        assertThat(header.value()).isEqualTo("1a8d6de63b7745ff8f4451db");
    }

    @Test
    public void canInfluenceDocumentFieldTypes() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_ID.name(), "event_id");
        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "payload_id");
        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_TYPE.name(), "event_type");
        config.put(MongoEventRouterConfigDefinition.FIELD_PAYLOAD.name(), "payload_body");
        config.put(MongoEventRouterConfigDefinition.ROUTE_BY_FIELD.name(), "my_route_field");
        config.put(MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "some_boolean:envelope:bool");
        router.configure(config);

        Document outboxEvent = new Document()
                .append("event_id", 2)
                .append("payload_id", 1232L)
                .append("event_type", "CoolSchemaCreated".getBytes())
                .append("payload_body", "{}".getBytes())
                .append("my_route_field", "routename")
                .append("some_boolean", true);

        JsonWriterSettings COMPACT_JSON_SETTINGS = JsonWriterSettings.builder()
                .outputMode(JsonMode.STRICT)
                .indent(true)
                .indentCharacters("")
                .newLineCharacters("")
                .build();

        String after = outboxEvent.toJson(COMPACT_JSON_SETTINGS);

        final Schema recordSchema = SchemaBuilder.struct()
                .name("event.Envelope")
                .field(Envelope.FieldName.AFTER, Json.builder().optional().build())
                // Change Streams field
                .field(MongoDbFieldName.UPDATE_DESCRIPTION, UPDATED_DESCRIPTION_SCHEMA)
                // .field(Envelope.FieldName.SOURCE, SchemaBuilder.struct().build())
                .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP_US, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP_NS, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                .build();

        final Envelope envelope = Envelope.fromSchema(recordSchema);

        final Struct body = envelope.create(after, null, Instant.now());
        final SourceRecord eventRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), body);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.topic()).isEqualTo("outbox.event.routename");

        // validate the valueSchema
        Schema valueSchema = eventRouted.valueSchema();
        assertThat(valueSchema.field("payload").schema().type()).isEqualTo(SchemaBuilder.bytes().type());
        assertThat(valueSchema.field("bool").schema().type()).isEqualTo(SchemaBuilder.bool().type());

        assertThat(((Struct) eventRouted.value()).get("payload")).isEqualTo("{}".getBytes());
        assertThat(eventRouted.key()).isEqualTo(1232L);

        Headers headers = eventRouted.headers();
        assertThat(headers.size()).isEqualTo(1);
        Header header = headers.iterator().next();
        assertThat(header.key()).isEqualTo("id");
        assertThat(header.value()).isEqualTo(2);
    }

    @Test
    public void canSetSchemaVersionWhenMoreThanPayloadIsInEnvelope() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), "version");
        config.put(MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "type:envelope:eventType");
        router.configure(config);

        Map<String, Object> extraValuesV1 = new HashMap<>();
        extraValuesV1.put("version", 1);

        final SourceRecord eventRecordV1 = createEventRecord(
                "000000000000000000000000",
                "UserCreated",
                "420b186d",
                "User",
                new Document(),
                extraValuesV1);
        final SourceRecord eventRoutedV1 = router.apply(eventRecordV1);

        assertThat(eventRoutedV1.valueSchema().version()).isEqualTo(1);

        Map<String, Object> extraValuesV3 = new HashMap<>();
        extraValuesV3.put("version", 3);

        final SourceRecord eventRecordV3 = createEventRecord(
                "000000000000000000000001",
                "UserCreated",
                "420b186d",
                "User",
                new Document(),
                extraValuesV3);
        final SourceRecord eventRoutedV3 = router.apply(eventRecordV3);

        assertThat(eventRoutedV3.valueSchema().version()).isEqualTo(3);

        // This one will now use the cached version
        final SourceRecord eventRecordV1E2 = createEventRecord(
                "000000000000000000000002",
                "UserCreated",
                "1b10b70b",
                "User",
                new Document(),
                extraValuesV1);
        final SourceRecord eventRoutedV1E2 = router.apply(eventRecordV1E2);

        assertThat(eventRoutedV1E2.valueSchema().version()).isEqualTo(1);

        assertThat(eventRoutedV1.valueSchema()).isSameAs(eventRoutedV1E2.valueSchema());
    }

    @Test
    public void shouldNotSetSchemaVersionByDefault() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), "version");
        router.configure(config);

        Map<String, Object> extraValuesV1 = new HashMap<>();
        extraValuesV1.put("version", 1);

        final SourceRecord eventRecordV1 = createEventRecord(
                "000000000000000000000000",
                "UserCreated",
                "420b186d",
                "User",
                new Document(),
                extraValuesV1);
        final SourceRecord eventRoutedV1 = router.apply(eventRecordV1);

        assertThat(eventRoutedV1.valueSchema().version()).isNull();

        Map<String, Object> extraValuesV3 = new HashMap<>();
        extraValuesV3.put("version", 3);

        final SourceRecord eventRecordV3 = createEventRecord(
                "000000000000000000000001",
                "UserCreated",
                "420b186d",
                "User",
                new Document(),
                extraValuesV3);
        final SourceRecord eventRoutedV3 = router.apply(eventRecordV3);

        assertThat(eventRoutedV3.valueSchema().version()).isNull();

        // This one will now use the cached version
        final SourceRecord eventRecordV1E2 = createEventRecord(
                "000000000000000000000002",
                "UserCreated",
                "1b10b70b",
                "User",
                new Document(),
                extraValuesV1);
        final SourceRecord eventRoutedV1E2 = router.apply(eventRecordV1E2);

        assertThat(eventRoutedV1E2.valueSchema().version()).isNull();
    }

    @Test
    public void canSetPayloadTypeIntoTheEnvelope() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "type:envelope");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(((Struct) eventRouted.value()).get("type")).isEqualTo("UserCreated");
    }

    @Test
    public void canSetPayloadTypeIntoTheEnvelopeWithAlias() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "type:envelope:aggregateType");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(((Struct) eventRouted.value()).get("aggregateType")).isEqualTo("UserCreated");
    }

    @Test
    public void canSetMultipleFieldsIntoTheEnvelope() {
        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "type:envelope:payloadType,aggregateid:envelope:payloadId,type:header:payloadType");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        Struct value = (Struct) eventRouted.value();
        assertThat(value.get("payloadType")).isEqualTo("UserCreated");
        assertThat(value.get("payloadId")).isEqualTo("000000000000000000000001");
        assertThat(eventRouted.headers().lastWithName("payloadType").value()).isEqualTo("UserCreated");
    }

    @Test(expected = ConfigException.class)
    public void shouldFailOnInvalidConfigurationForTopicRegex() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.ROUTE_TOPIC_REGEX.name(), " [[a-z]");
        router.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailOnInvalidConfigurationForAdditionalFields() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "type");
        router.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailOnInvalidConfigurationForAdditionalFieldsEmpty() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "");
        router.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailOnInvalidConfigurationForOperationBehavior() {
        final Map<String, String> config = new HashMap<>();
        config.put(MongoEventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR.name(), "invalidOption");
        router.configure(config);
    }

    @Test
    public void canSetBinaryMessageKey() {
        final byte[] eventType = "a UserCreated".getBytes(StandardCharsets.UTF_8);

        final Map<String, String> config = new HashMap<>();
        // This is not a good example of message key, this is just for test
        config.put(MongoEventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "type");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de63b7745ff8f4457db",
                eventType,
                "Some other payload id",
                "User",
                new Document(),
                new HashMap<>());

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.BYTES);
        assertThat(eventRouted.key()).isEqualTo(eventType);
    }

    @Test
    public void canPassBinaryKey() {
        final byte[] key = "a binary key".getBytes(StandardCharsets.UTF_8);
        canPassKeyByType(SchemaBuilder.bytes(), key);
    }

    @Test
    public void canPassIntKey() {
        final int key = 54321;
        canPassKeyByType(SchemaBuilder.int32(), key);
    }

    private void canPassKeyByType(SchemaBuilder keyType, Object key) {
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de63b7745ff8f4457db",
                "UserCreated",
                key,
                "User",
                new Document(),
                new HashMap<>());
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(keyType.type());
        assertThat(eventRouted.key()).isEqualTo(key);
    }

    @Test
    public void canPassBinaryMessage() {
        final byte[] value = "a binary message".getBytes(StandardCharsets.UTF_8);
        final String key = "a key";

        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de63b7745ff8f4457db",
                "UserCreated",
                key,
                "User",
                value,
                new HashMap<>());
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(eventRouted.key()).isEqualTo(key);
        assertThat(eventRouted.valueSchema().type()).isEqualTo(Schema.Type.BYTES);
        assertThat(eventRouted.value()).isEqualTo(value);
    }

    @Test
    public void canMarkAnEventAsDeleted() {
        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "is_deleted:envelope:deleted");
        config.put(
                MongoEventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name(),
                "true");
        router.configure(config);

        final Map<String, Object> extraValues = new HashMap<>();
        extraValues.put("is_deleted", true);

        final SourceRecord eventRecord = createEventRecord(
                "000000000000000000000000",
                "UserCreated",
                "10711fa5",
                "User",
                new Document(),
                extraValues);
        final SourceRecord eventRouted = router.apply(eventRecord);

        Struct value = (Struct) eventRouted.value();
        assertThat(value).isNotNull();
        assertThat(value.get("deleted")).isEqualTo(true);

        final SourceRecord eventRecordTombstone = createEventRecord(
                "000000000000000000000000",
                "UserCreated",
                "10711fa5",
                "User",
                null,
                extraValues);
        final SourceRecord eventRoutedTombstone = router.apply(eventRecordTombstone);

        Struct tombstone = (Struct) eventRoutedTombstone.value();
        assertThat(tombstone).isNull();
        VerifyRecord.isValidTombstone(eventRoutedTombstone);
    }

    @Test
    public void noTombstoneIfNotConfigured() {
        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "is_deleted:envelope:deleted");
        router.configure(config);

        final Map<String, Object> extraValues = new HashMap<>();
        extraValues.put("is_deleted", true);

        final SourceRecord eventRecord = createEventRecord(
                "000000000000000000000000",
                "UserCreated",
                "10711fa5",
                "User",
                new Document(),
                extraValues);
        final SourceRecord eventRouted = router.apply(eventRecord);

        Struct value = (Struct) eventRouted.value();
        assertThat(value).isNotNull();
        assertThat(value.get("deleted")).isEqualTo(true);

        final SourceRecord eventRecordTombstone = createEventRecord(
                "000000000000000000000001",
                "UserCreated",
                "10711fa5",
                "User",
                null,
                extraValues);
        final SourceRecord eventRoutedTombstone = router.apply(eventRecordTombstone);

        Struct tombstone = (Struct) eventRoutedTombstone.value();
        assertThat(eventRoutedTombstone.key()).isNotNull();
        assertThat(eventRoutedTombstone.keySchema()).isNotNull();
        assertThat(tombstone).isNotNull();
        assertThat(tombstone.get("deleted")).isEqualTo(true);
        assertThat(eventRoutedTombstone.valueSchema()).isNotNull();
    }

    @Test
    public void canExpandJsonPayloadIfConfigured() {
        final Map<String, String> config = new HashMap<>();
        config.put(
                MongoEventRouterConfigDefinition.EXPAND_JSON_PAYLOAD.name(),
                "true");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "000000000000000000000000",
                "UserCreated",
                new ObjectId("000000000000000000000001"),
                "User",
                new Document()
                        .append("fullName", "John Doe")
                        .append("enabled", true)
                        .append("rating", 4.9)
                        .append("age", 42L)
                        .append("pets", Arrays.asList("dog", "cat")));
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();

        Schema valueSchema = eventRouted.valueSchema();
        assertThat(valueSchema.type()).isEqualTo(SchemaBuilder.struct().type());

        assertThat(valueSchema.fields().size()).isEqualTo(5);
        assertThat(valueSchema.field("fullName").schema().type().getName()).isEqualTo("string");
        assertThat(valueSchema.field("enabled").schema().type().getName()).isEqualTo("boolean");
        assertThat(valueSchema.field("rating").schema().type().getName()).isEqualTo("float64");
        assertThat(valueSchema.field("age").schema().type().getName()).isEqualTo("int64");
        assertThat(valueSchema.field("pets").schema().type().getName()).isEqualTo("array");

        Struct valueStruct = (Struct) eventRouted.value();
        assertThat(valueStruct.get("fullName")).isEqualTo("John Doe");
        assertThat(valueStruct.get("enabled")).isEqualTo(true);
        assertThat(valueStruct.get("rating")).isEqualTo(4.9);
        assertThat(valueStruct.get("age")).isEqualTo(42L);
        assertThat(valueStruct.getArray("pets").size()).isEqualTo(2);
        assertThat(valueStruct.getArray("pets").get(1)).isEqualTo("cat");
    }

    @Test
    public void shouldNotExpandJSONPayloadIfNotConfigured() {
        router.configure(new HashMap<>());

        final SourceRecord eventRecord = createEventRecord(
                "000000000000000000000000",
                "UserCreated",
                new ObjectId("000000000000000000000001"),
                "User",
                new Document()
                        .append("fullName", "John Doe")
                        .append("rating", 4.9)
                        .append("age", 42));

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();

        assertThat(eventRouted.valueSchema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        Document payload = Document.parse((String) eventRouted.value());
        assertThat(payload.get("fullName")).isEqualTo("John Doe");
        assertThat(payload.get("rating")).isEqualTo(4.9);
        assertThat(payload.get("age")).isEqualTo(42);
    }

    private SourceRecord createEventRecord() {
        return createEventRecord(
                "da8d6de63b7745ff8f4457db",
                "UserCreated",
                new ObjectId("000000000000000000000001"),
                "User",
                new Document());
    }

    private SourceRecord createEventRecord(
                                           String eventId,
                                           String eventType,
                                           ObjectId payloadId,
                                           String payloadType,
                                           Object payload) {
        return createEventRecord(
                eventId,
                eventType,
                payloadId,
                payloadType,
                payload,
                new HashMap<>());
    }

    private SourceRecord createEventRecord(
                                           String eventId,
                                           Object eventType,
                                           Object payloadId,
                                           Object payloadType,
                                           Object payload,
                                           Map<String, Object> extraValues) {
        Document outboxEvent = new Document()
                .append("_id", new ObjectId(eventId))
                .append("aggregatetype", payloadType)
                .append("aggregateid", payloadId)
                .append("type", eventType)
                .append("payload", payload);

        extraValues.forEach(outboxEvent::append);

        String after = outboxEvent.toJson(COMPACT_JSON_SETTINGS);

        final Schema valueSchema = SchemaBuilder.struct()
                .name("event.Envelope")
                .field(Envelope.FieldName.AFTER, Json.builder().optional().build())
                // Change Streams field
                .field(MongoDbFieldName.UPDATE_DESCRIPTION, UPDATED_DESCRIPTION_SCHEMA)
                // .field(Envelope.FieldName.SOURCE, SchemaBuilder.struct().build())
                .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP_US, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP_NS, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                .build();

        final Envelope envelope = Envelope.fromSchema(valueSchema);

        final Struct body = envelope.create(after, null, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), body);
    }
}
