/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.time.Timestamp;

/**
 * Unit tests for {@link EventRouter}
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouterTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void canSkipTombstone() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
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
        final EventRouter<SourceRecord> router = new EventRouter<>();
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
        final Struct payload = envelope.delete(before, null, Instant.now());
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

    @Test
    @FixFor("DBZ-1383")
    public void canSkipMessagesWithoutDebeziumCdcEnvelopeDueToMissingSchemaName() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
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
        final EventRouter<SourceRecord> router = new EventRouter<>();
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

        exceptionRule.expect(DataException.class);
        exceptionRule.expectMessage("op is not a valid field name");

        router.apply(eventRecord);
    }

    @Test
    @FixFor("DBZ-1383")
    public void canSkipMessagesWithoutDebeziumCdcEnvelopeDueToMissingSchemaNameSuffix() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
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
    @FixFor("DBZ-1383")
    public void canSkipMessagesWithoutDebeziumCdcEnvelopeDueToMissingValueSchema() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
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
        final EventRouter<SourceRecord> router = new EventRouter<>();
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
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR.name(),
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
        final EventRouter<SourceRecord> router = new EventRouter<>();
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
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(eventRouted.key()).isEqualTo("10711fa5");
    }

    @Test
    public void canSetMessageKey() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        // This is not a good example of message key, this is just for test
        config.put(EventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "type");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(eventRouted.key()).isEqualTo("UserCreated");
    }

    @Test(expected = DataException.class)
    public void failsOnInvalidSetMessageKey() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "fakefield");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        router.apply(eventRecord);
    }

    @Test
    public void canSetTimestampFromDebeziumEnvelopeByDefault() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
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
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), "event_timestamp");
        router.configure(config);

        Long expectedTimestamp = 14222264625338L;

        Map<String, Schema> extraFields = new HashMap<>();
        extraFields.put("event_timestamp", Timestamp.schema());

        Map<String, Object> extraValues = new HashMap<>();
        extraValues.put("event_timestamp", expectedTimestamp);

        final SourceRecord userEventRecord = createEventRecord(
                "166080d9-3b0e-4a04-81fe-2058a7386f1f",
                "UserCreated",
                "420b186d",
                "User",
                "{}",
                extraFields,
                extraValues);
        final SourceRecord userEventRouted = router.apply(userEventRecord);

        assertThat(userEventRecord.timestamp()).isNull();
        assertThat(userEventRouted.timestamp()).isEqualTo(expectedTimestamp);
    }

    @Test
    public void canRouteBasedOnField() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.ROUTE_BY_FIELD.name(),
                "aggregatetype");
        router.configure(config);

        final SourceRecord userEventRecord = createEventRecord();
        final SourceRecord userEventRouted = router.apply(userEventRecord);

        assertThat(userEventRouted).isNotNull();
        assertThat(userEventRouted.topic()).isEqualTo("outbox.event.User");

        final SourceRecord userUpdatedEventRecord = createEventRecord(
                "ab720dd3-176d-40a6-96f3-6cf961d7df6a",
                "UserUpdate",
                "10711fa5",
                "User",
                "{}");
        final SourceRecord userUpdatedEventRouted = router.apply(userUpdatedEventRecord);

        assertThat(userUpdatedEventRouted).isNotNull();
        assertThat(userUpdatedEventRouted.topic()).isEqualTo("outbox.event.User");

        final SourceRecord addressCreatedEventRecord = createEventRecord(
                "ab720dd3-176d-40a6-96f3-6cf961d7df6a",
                "AddressCreated",
                "10711fa5",
                "Address",
                "{}");
        final SourceRecord addressCreatedEventRouted = router.apply(addressCreatedEventRecord);

        assertThat(addressCreatedEventRouted).isNotNull();
        assertThat(addressCreatedEventRouted.topic()).isEqualTo("outbox.event.Address");
    }

    @Test
    public void canConfigureEveryTableField() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_EVENT_ID.name(), "event_id");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "payload_id");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TYPE.name(), "event_type");
        config.put(EventRouterConfigDefinition.FIELD_PAYLOAD.name(), "payload_body");
        config.put(EventRouterConfigDefinition.ROUTE_BY_FIELD.name(), "payload_id");
        router.configure(config);

        final Schema recordSchema = SchemaBuilder.struct()
                .field("event_id", SchemaBuilder.string())
                .field("payload_id", SchemaBuilder.string())
                .field("event_type", SchemaBuilder.string())
                .field("payload_body", SchemaBuilder.string())
                .build();

        Envelope envelope = Envelope.defineSchema()
                .withName("event.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();

        final Struct before = new Struct(recordSchema);
        before.put("event_id", "da8d6de6-3b77-45ff-8f44-57db55a7a06c");
        before.put("payload_id", "10711fa5");
        before.put("event_type", "UserCreated");
        before.put("payload_body", "{}");

        final Struct payload = envelope.create(before, null, Instant.now());
        final SourceRecord eventRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), payload);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.value()).isEqualTo("{}");

        Headers headers = eventRouted.headers();
        assertThat(headers.size()).isEqualTo(1);
        Header header = headers.iterator().next();
        assertThat(header.key()).isEqualTo("id");
        assertThat(header.value()).isEqualTo("da8d6de6-3b77-45ff-8f44-57db55a7a06c");
    }

    @Test
    public void canInfluenceTableColumnTypes() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_EVENT_ID.name(), "event_id");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "payload_id");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TYPE.name(), "event_type");
        config.put(EventRouterConfigDefinition.FIELD_PAYLOAD.name(), "payload_body");
        config.put(EventRouterConfigDefinition.ROUTE_BY_FIELD.name(), "my_route_field");
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "some_boolean:envelope:bool");
        router.configure(config);

        final Schema recordSchema = SchemaBuilder.struct()
                .field("event_id", SchemaBuilder.int32())
                .field("payload_id", SchemaBuilder.int32())
                .field("my_route_field", SchemaBuilder.string())
                .field("event_type", SchemaBuilder.bytes())
                .field("payload_body", SchemaBuilder.bytes())
                .field("some_boolean", SchemaBuilder.bool())
                .build();

        Envelope envelope = Envelope.defineSchema()
                .withName("event.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();

        final Struct before = new Struct(recordSchema);
        before.put("event_id", 2);
        before.put("payload_id", 1232);
        before.put("event_type", "CoolSchemaCreated".getBytes());
        before.put("my_route_field", "routename");
        before.put("payload_body", "{}".getBytes());
        before.put("some_boolean", true);

        final Struct payload = envelope.create(before, null, Instant.now());
        final SourceRecord eventRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), payload);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.topic()).isEqualTo("outbox.event.routename");

        // validate the valueSchema
        Schema valueSchema = eventRouted.valueSchema();
        assertThat(valueSchema.field("payload").schema().type()).isEqualTo(SchemaBuilder.bytes().type());
        assertThat(valueSchema.field("bool").schema().type()).isEqualTo(SchemaBuilder.bool().type());

        assertThat(((Struct) eventRouted.value()).get("payload")).isEqualTo("{}".getBytes());
        assertThat(eventRouted.key()).isEqualTo(1232);

        Headers headers = eventRouted.headers();
        assertThat(headers.size()).isEqualTo(1);
        Header header = headers.iterator().next();
        assertThat(header.key()).isEqualTo("id");
        assertThat(header.value()).isEqualTo(2);
    }

    @Test
    public void canSetSchemaVersionWhenMoreThanPayloadIsInEnvelope() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), "version");
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "type:envelope:eventType");
        router.configure(config);

        Map<String, Schema> extraFields = new HashMap<>();
        extraFields.put("version", Schema.INT32_SCHEMA);

        Map<String, Object> extraValuesV1 = new HashMap<>();
        extraValuesV1.put("version", 1);

        final SourceRecord eventRecordV1 = createEventRecord(
                "166080d9-3b0e-4a04-81fe-2058a7386f1f",
                "UserCreated",
                "420b186d",
                "User",
                "{}",
                extraFields,
                extraValuesV1);
        final SourceRecord eventRoutedV1 = router.apply(eventRecordV1);

        assertThat(eventRoutedV1.valueSchema().version()).isEqualTo(1);

        Map<String, Object> extraValuesV3 = new HashMap<>();
        extraValuesV3.put("version", 3);

        final SourceRecord eventRecordV3 = createEventRecord(
                "166080d9-3b0e-4a04-81fe-2058a7386f1f",
                "UserCreated",
                "420b186d",
                "User",
                "{}",
                extraFields,
                extraValuesV3);
        final SourceRecord eventRoutedV3 = router.apply(eventRecordV3);

        assertThat(eventRoutedV3.valueSchema().version()).isEqualTo(3);

        // This one will now use the cached version
        final SourceRecord eventRecordV1E2 = createEventRecord(
                "18f94a39-b931-41b7-837c-6fc23b013597",
                "UserCreated",
                "1b10b70b",
                "User",
                "{}",
                extraFields,
                extraValuesV1);
        final SourceRecord eventRoutedV1E2 = router.apply(eventRecordV1E2);

        assertThat(eventRoutedV1E2.valueSchema().version()).isEqualTo(1);

        assertThat(eventRoutedV1.valueSchema()).isSameAs(eventRoutedV1E2.valueSchema());
    }

    @Test
    public void shouldNotSetSchemaVersionByDefault() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), "version");
        router.configure(config);

        Map<String, Schema> extraFields = new HashMap<>();
        extraFields.put("version", Schema.INT32_SCHEMA);

        Map<String, Object> extraValuesV1 = new HashMap<>();
        extraValuesV1.put("version", 1);

        final SourceRecord eventRecordV1 = createEventRecord(
                "166080d9-3b0e-4a04-81fe-2058a7386f1f",
                "UserCreated",
                "420b186d",
                "User",
                "{}",
                extraFields,
                extraValuesV1);
        final SourceRecord eventRoutedV1 = router.apply(eventRecordV1);

        assertThat(eventRoutedV1.valueSchema().version()).isNull();

        Map<String, Object> extraValuesV3 = new HashMap<>();
        extraValuesV3.put("version", 3);

        final SourceRecord eventRecordV3 = createEventRecord(
                "166080d9-3b0e-4a04-81fe-2058a7386f1f",
                "UserCreated",
                "420b186d",
                "User",
                "{}",
                extraFields,
                extraValuesV3);
        final SourceRecord eventRoutedV3 = router.apply(eventRecordV3);

        assertThat(eventRoutedV3.valueSchema().version()).isNull();

        // This one will now use the cached version
        final SourceRecord eventRecordV1E2 = createEventRecord(
                "18f94a39-b931-41b7-837c-6fc23b013597",
                "UserCreated",
                "1b10b70b",
                "User",
                "{}",
                extraFields,
                extraValuesV1);
        final SourceRecord eventRoutedV1E2 = router.apply(eventRecordV1E2);

        assertThat(eventRoutedV1E2.valueSchema().version()).isNull();
    }

    @Test
    public void canSetPayloadTypeIntoTheEnvelope() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "type:envelope");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(((Struct) eventRouted.value()).get("type")).isEqualTo("UserCreated");
    }

    @Test
    public void canSetPayloadTypeIntoTheEnvelopeWithAlias() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "type:envelope:aggregateType");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(((Struct) eventRouted.value()).get("aggregateType")).isEqualTo("UserCreated");
    }

    @Test
    public void canSetMultipleFieldsIntoTheEnvelope() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "type:envelope:payloadType, aggregateid:envelope:payloadId,type:header:payloadType");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        Struct value = (Struct) eventRouted.value();
        assertThat(value.get("payloadType")).isEqualTo("UserCreated");
        assertThat(value.get("payloadId")).isEqualTo("10711fa5");
        assertThat(eventRouted.headers().lastWithName("payloadType").value()).isEqualTo("UserCreated");
    }

    @Test
    public void canSetPartitionWithAdditionalFields() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_EVENT_ID.name(), "event_id");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "payload_id");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TYPE.name(), "event_type");
        config.put(EventRouterConfigDefinition.FIELD_PAYLOAD.name(), "payload_body");
        config.put(EventRouterConfigDefinition.ROUTE_BY_FIELD.name(), "my_route_field");
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "p:partition");
        router.configure(config);

        final Schema recordSchema = SchemaBuilder.struct()
                .field("event_id", SchemaBuilder.int32())
                .field("payload_id", SchemaBuilder.int32())
                .field("my_route_field", SchemaBuilder.string())
                .field("event_type", SchemaBuilder.bytes())
                .field("payload_body", SchemaBuilder.bytes())
                .field("p", SchemaBuilder.int32())
                .build();

        Envelope envelope = Envelope.defineSchema()
                .withName("event.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();

        final Struct before = new Struct(recordSchema);
        before.put("event_id", 2);
        before.put("payload_id", 1232);
        before.put("event_type", "CoolSchemaCreated".getBytes());
        before.put("my_route_field", "routename");
        before.put("payload_body", "{}".getBytes());
        before.put("p", 123);

        final Struct payload = envelope.create(before, null, Instant.now());
        final SourceRecord eventRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), payload);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.kafkaPartition()).isEqualTo(123);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailOnInvalidConfigurationForTopicRegex() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.ROUTE_TOPIC_REGEX.name(), " [[a-z]**");
        router.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailOnInvalidConfigurationForAdditionalFields() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "type");
        router.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailOnInvalidConfigurationForAdditionalFieldsEmpty() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "");
        router.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailOnInvalidConfigurationForOperationBehavior() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR.name(), "invalidOption");
        router.configure(config);
    }

    @Test
    @FixFor("DBZ-2152")
    public void canPassStringKey() {
        // canSetDefaultMessageKey() duplicates this logic, as the current test assumes the key will be a String
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    @FixFor("DBZ-2152")
    public void canSetBinaryMessageKey() {
        final byte[] eventType = "a UserCreated".getBytes(StandardCharsets.UTF_8);
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        // This is not a good example of message key, this is just for test
        config.put(EventRouterConfigDefinition.FIELD_EVENT_KEY.name(), "type");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                SchemaBuilder.bytes(),
                eventType,
                SchemaBuilder.string(),
                "Some other payload id",
                "User",
                SchemaBuilder.string(),
                "{}",
                new HashMap<>(),
                new HashMap<>());

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(Schema.Type.BYTES);
        assertThat(eventRouted.key()).isEqualTo(eventType);
    }

    @Test
    @FixFor("DBZ-2152")
    public void canPassBinaryKey() {
        final byte[] key = "a binary key".getBytes(StandardCharsets.UTF_8);
        canPassKeyByType(SchemaBuilder.bytes(), key);
    }

    @Test
    @FixFor("DBZ-2152")
    public void canPassIntKey() {
        final int key = 54321;
        canPassKeyByType(SchemaBuilder.int32(), key);
    }

    private void canPassKeyByType(SchemaBuilder keyType, Object key) {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                SchemaBuilder.string(),
                "UserCreated",
                keyType,
                key,
                "User",
                SchemaBuilder.string(),
                "{}",
                new HashMap<>(),
                new HashMap<>());
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.keySchema().type()).isEqualTo(keyType.type());
        assertThat(eventRouted.key()).isEqualTo(key);
    }

    @Test
    @FixFor("DBZ-2152")
    public void canPassBinaryMessage() {
        final byte[] value = "a binary message".getBytes(StandardCharsets.UTF_8);
        final String key = "a key";
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                SchemaBuilder.string(),
                "UserCreated",
                SchemaBuilder.string(),
                key,
                "User",
                SchemaBuilder.bytes(),
                value,
                new HashMap<>(),
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
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "is_deleted:envelope:deleted");
        config.put(
                EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name(),
                "true");
        router.configure(config);

        final Map<String, Schema> extraFields = new HashMap<>();
        extraFields.put("deleted", Schema.OPTIONAL_BOOLEAN_SCHEMA);

        final Map<String, Object> extraValues = new HashMap<>();
        extraValues.put("is_deleted", true);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "{}",
                extraFields,
                extraValues);
        final SourceRecord eventRouted = router.apply(eventRecord);

        Struct value = (Struct) eventRouted.value();
        assertThat(value).isNotNull();
        assertThat(value.get("deleted")).isEqualTo(true);

        final SourceRecord eventRecordTombstone = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "",
                extraFields,
                extraValues);
        final SourceRecord eventRoutedTombstone = router.apply(eventRecordTombstone);

        Struct tombstone = (Struct) eventRoutedTombstone.value();
        assertThat(tombstone).isNull();
        VerifyRecord.isValidTombstone(eventRoutedTombstone);
    }

    @Test
    public void noTombstoneIfNotConfigured() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "is_deleted:envelope:deleted");
        router.configure(config);

        final Map<String, Schema> extraFields = new HashMap<>();
        extraFields.put("deleted", Schema.OPTIONAL_BOOLEAN_SCHEMA);

        final Map<String, Object> extraValues = new HashMap<>();
        extraValues.put("is_deleted", true);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "{}",
                extraFields,
                extraValues);
        final SourceRecord eventRouted = router.apply(eventRecord);

        Struct value = (Struct) eventRouted.value();
        assertThat(value).isNotNull();
        assertThat(value.get("deleted")).isEqualTo(true);

        final SourceRecord eventRecordTombstone = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "",
                extraFields,
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
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.EXPAND_JSON_PAYLOAD.name(),
                "true");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "{\"fullName\": \"John Doe\", \"enabled\": true, \"rating\": 4.9, \"age\": 42, \"pets\": [\"dog\", \"cat\"], \"petObjects\": [{\"type\": \"dog\"}, {\"type\": \"cat\"}]}",
                new HashMap<>(),
                new HashMap<>());
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();

        Schema valueSchema = eventRouted.valueSchema();
        assertThat(valueSchema.type()).isEqualTo(SchemaBuilder.struct().type());

        assertThat(valueSchema.fields().size()).isEqualTo(6);
        assertThat(valueSchema.field("fullName").schema().type().getName()).isEqualTo("string");
        assertThat(valueSchema.field("enabled").schema().type().getName()).isEqualTo("boolean");
        assertThat(valueSchema.field("rating").schema().type().getName()).isEqualTo("float64");
        assertThat(valueSchema.field("age").schema().type().getName()).isEqualTo("int32");
        assertThat(valueSchema.field("pets").schema().type().getName()).isEqualTo("array");

        Struct valueStruct = (Struct) eventRouted.value();
        assertThat(valueStruct.get("fullName")).isEqualTo("John Doe");
        assertThat(valueStruct.get("enabled")).isEqualTo(true);
        assertThat(valueStruct.get("rating")).isEqualTo(4.9);
        assertThat(valueStruct.get("age")).isEqualTo(42);
        assertThat(valueStruct.getArray("pets").size()).isEqualTo(2);
        assertThat(valueStruct.getArray("pets").get(1)).isEqualTo("cat");

        List<Object> petObjects = valueStruct.getArray("petObjects");
        assertThat(petObjects.size()).isEqualTo(2);
        assertThat(asStruct(petObjects.get(0)).get("type")).isEqualTo("dog");
        assertThat(asStruct(petObjects.get(1)).get("type")).isEqualTo("cat");
    }

    @Test
    public void canExpandJsonWithNestedArraysWhereFirstArrayIsEmpty() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.EXPAND_JSON_PAYLOAD.name(),
                "true");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "{\"fullName\": \"John Doe\", \"petObjects\": [{\"type\": \"dog\", \"colors\": []}, {\"type\": \"cat\", \"colors\": [{\"name\": \"white\"}]}]}",
                new HashMap<>(),
                new HashMap<>());
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();

        Schema valueSchema = eventRouted.valueSchema();
        assertThat(valueSchema.type()).isEqualTo(SchemaBuilder.struct().type());

        assertThat(valueSchema.fields().size()).isEqualTo(2);
        assertThat(valueSchema.field("fullName").schema().type().getName()).isEqualTo("string");
        assertThat(valueSchema.field("petObjects").schema().type().getName()).isEqualTo("array");
        assertThat(valueSchema.field("petObjects").schema().valueSchema().fields().size()).isEqualTo(2);
        assertThat(valueSchema.field("petObjects").schema().valueSchema().type().getName()).isEqualTo("struct");
        assertThat(valueSchema.field("petObjects").schema().valueSchema().field("type").schema().type().getName()).isEqualTo("string");
        assertThat(valueSchema.field("petObjects").schema().valueSchema().field("colors").schema().type().getName()).isEqualTo("array");
        assertThat(valueSchema.field("petObjects").schema().valueSchema().field("colors").schema().valueSchema().type().getName()).isEqualTo("struct");
        assertThat(valueSchema.field("petObjects").schema().valueSchema().field("colors").schema().valueSchema().fields().size()).isEqualTo(1);
        assertThat(valueSchema.field("petObjects").schema().valueSchema().field("colors").schema().valueSchema().field("name").schema().type().getName())
                .isEqualTo("string");

        Struct valueStruct = (Struct) eventRouted.value();
        assertThat(valueStruct.get("fullName")).isEqualTo("John Doe");
        List<Object> petObjects = valueStruct.getArray("petObjects");
        assertThat(petObjects.size()).isEqualTo(2);
        assertThat(asStruct(petObjects.get(0)).get("type")).isEqualTo("dog");
        assertThat(asStruct(petObjects.get(0)).getArray("colors")).hasSize(0);
        assertThat(asStruct(petObjects.get(1)).get("type")).isEqualTo("cat");
        assertThat(asStruct(petObjects.get(1)).getArray("colors")).hasSize(1);
        List<Object> colors = asStruct(petObjects.get(1)).getArray("colors");
        assertThat(asStruct(colors.get(0)).get("name")).isEqualTo("white");
    }

    @Test
    public void shouldExpandJSONPayloadWithEmptyArrayAndRemoveThatArray() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.EXPAND_JSON_PAYLOAD.name(),
                "true");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "{\"fullName\": \"John Doe\", \"petObjects\": [{\"type\": \"dog\", \"colors\": []}]}",
                new HashMap<>(),
                new HashMap<>());
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();

        Schema valueSchema = eventRouted.valueSchema();
        assertThat(valueSchema.type()).isEqualTo(SchemaBuilder.struct().type());

        assertThat(valueSchema.fields().size()).isEqualTo(2);
        assertThat(valueSchema.field("fullName").schema().type().getName()).isEqualTo("string");
        assertThat(valueSchema.field("petObjects").schema().type().getName()).isEqualTo("array");
        assertThat(valueSchema.field("petObjects").schema().valueSchema().fields().size()).isEqualTo(1);
        assertThat(valueSchema.field("petObjects").schema().valueSchema().field("type").schema().type().getName()).isEqualTo("string");

        Struct valueStruct = (Struct) eventRouted.value();
        assertThat(valueStruct.get("fullName")).isEqualTo("John Doe");
        List<Object> petObjects = valueStruct.getArray("petObjects");
        assertThat(petObjects.size()).isEqualTo(1);
        assertThat(asStruct(petObjects.get(0)).get("type")).isEqualTo("dog");
    }

    @Test
    public void shouldNotExpandJSONPayloadIfNotConfigured() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        router.configure(new HashMap<>());

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "{\"fullName\": \"John Doe\", \"rating\": 4.9, \"age\": 42}",
                new HashMap<>(),
                new HashMap<>());

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(eventRouted.valueSchema().type()).isEqualTo(SchemaBuilder.string().type());
        assertThat(eventRouted.value()).isEqualTo("{\"fullName\": \"John Doe\", \"rating\": 4.9, \"age\": 42}");
    }

    @Test
    public void canExpandJsonPayloadWithAdditionalFieldInEnvelope() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.EXPAND_JSON_PAYLOAD.name(),
                "true");
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "type:envelope");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "{\"fullName\": \"John Doe\"}",
                new HashMap<>(),
                new HashMap<>());
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();

        Schema valueSchema = eventRouted.valueSchema();
        assertThat(valueSchema.type()).isEqualTo(SchemaBuilder.struct().type());
        assertThat(valueSchema.fields().size()).isEqualTo(2);
        assertThat(valueSchema.field("type").schema().type().getName()).isEqualTo("string");

        Schema payloadSchema = valueSchema.field("payload").schema();
        assertThat(payloadSchema.type()).isEqualTo(SchemaBuilder.struct().type());
        assertThat(payloadSchema.fields().size()).isEqualTo(1);
        assertThat(payloadSchema.field("fullName").schema().type().getName()).isEqualTo("string");

        Struct valueStruct = (Struct) eventRouted.value();
        assertThat(valueStruct.get("type")).isEqualTo("UserCreated");

        Struct payloadStruct = valueStruct.getStruct("payload");
        assertThat(payloadStruct.get("fullName")).isEqualTo("John Doe");
    }

    @Test
    public void canExpandJsonArrayWithFirstElementNull() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.EXPAND_JSON_PAYLOAD.name(),
                "true");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "{\"fullName\": \"John Doe\", \"numbers\": [null, 2, 3]}",
                new HashMap<>(),
                new HashMap<>());
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();

        Schema valueSchema = eventRouted.valueSchema();
        assertThat(valueSchema.type()).isEqualTo(SchemaBuilder.struct().type());

        assertThat(valueSchema.fields().size()).isEqualTo(2);
        assertThat(valueSchema.field("fullName").schema().type().getName()).isEqualTo("string");
        assertThat(valueSchema.field("numbers").schema().type().getName()).isEqualTo("array");
        assertThat(valueSchema.field("numbers").schema().valueSchema().type().getName()).isEqualTo("int32");

        Struct valueStruct = (Struct) eventRouted.value();
        assertThat(valueStruct.get("fullName")).isEqualTo("John Doe");
        List<Object> numbers = valueStruct.getArray("numbers");
        assertThat(numbers.size()).isEqualTo(3);
        assertThat(numbers.get(0)).isEqualTo(null);
        assertThat(numbers.get(1)).isEqualTo(2);
        assertThat(numbers.get(2)).isEqualTo(3);
    }

    @Test
    public void canExpandJsonArrayWithComplexElements() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(
                EventRouterConfigDefinition.EXPAND_JSON_PAYLOAD.name(),
                "true");
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord(
                "59875832-7f44-11ed-954e-1a5354ab5dd0|",
                "UserCreated",
                "6a3f12a0",
                "User",
                "{\"id\":\"1028cbab-c55b-4dab-a367-7d9471466a59\",\"attributes\":[{\"id\":\"10911586\",\"adr\":null,\"eans\":[{\"code\":\"2007000013656\",\"createdAt\":\"2022-06-08T11:33:16.465Z\"}],\"purchaseBusinessStatus\":\"BLOCOM\",\"isCanBeReturnToSupplier\":false,\"purchaseWarehouseStatus\":\"NONAUT\"}]}",
                new HashMap<>(),
                new HashMap<>());
        SourceRecord eventRouted = router.apply(eventRecord);
        assertThat(eventRouted).isNotNull();
        Struct valueStruct = (Struct) eventRouted.value();
        List<Struct> eans = (ArrayList) ((Struct) ((ArrayList) valueStruct.get("attributes")).get(0)).get("eans");
        assertThat(eans.size()).isEqualTo(1);

        final SourceRecord eventRecord2 = createEventRecord(
                "59875832-7f44-11ed-954e-1a5354ab5dd0|",
                "UserCreated",
                "6a3f12a0",
                "User",
                "{\"id\":\"1028cbab-c55b-4dab-a367-7d9471466a59\",\"attributes\":[{\"id\":\"10911586\",\"adr\":null,\"eans\":[{\"code\":\"2007000013656\",\"createdAt\":\"2022-06-08T11:33:16.465Z\"},{\"code\":\"2000630108556\",\"createdAt\":\"2022-06-23T07:57:54.717Z\"}],\"purchaseBusinessStatus\":\"BLOCOM\",\"isCanBeReturnToSupplier\":false,\"purchaseWarehouseStatus\":\"NONAUT\"}]}",
                new HashMap<>(),
                new HashMap<>());
        SourceRecord eventRouted2 = router.apply(eventRecord2);
        assertThat(eventRouted2).isNotNull();
        Struct valueStruct2 = (Struct) eventRouted2.value();
        List<Struct> eans2 = (ArrayList) ((Struct) ((ArrayList) valueStruct2.get("attributes")).get(0)).get("eans");
        assertThat(eans2.size()).isEqualTo(2);

        assertThat(eventRouted.valueSchema()).isEqualTo(eventRouted2.valueSchema());
    }

    private SourceRecord createEventRecord() {
        return createEventRecord(
                "da8d6de6-3b77-45ff-8f44-57db55a7a06c",
                "UserCreated",
                "10711fa5",
                "User",
                "{}");
    }

    private SourceRecord createEventRecord(
                                           String eventId,
                                           String eventType,
                                           String payloadId,
                                           String payloadType,
                                           String payload) {
        return createEventRecord(
                eventId,
                eventType,
                payloadId,
                payloadType,
                payload,
                new HashMap<>(),
                new HashMap<>());
    }

    private SourceRecord createEventRecord(
                                           String eventId,
                                           String eventType,
                                           String payloadId,
                                           String payloadType,
                                           String payload,
                                           Map<String, Schema> extraFields,
                                           Map<String, Object> extraValues) {
        return createEventRecord(
                eventId,
                SchemaBuilder.string(),
                eventType,
                SchemaBuilder.string(),
                payloadId,
                payloadType,
                SchemaBuilder.string(),
                payload,
                extraFields,
                extraValues);
    }

    private SourceRecord createEventRecord(
                                           String eventId,
                                           SchemaBuilder eventTypeSchemaType,
                                           Object eventType,
                                           SchemaBuilder payloadIdSchemaType,
                                           Object payloadId,
                                           String payloadType,
                                           SchemaBuilder payloadSchemaType,
                                           Object payload,
                                           Map<String, Schema> extraFields,
                                           Map<String, Object> extraValues) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .field("id", SchemaBuilder.string())
                .field("aggregatetype", SchemaBuilder.string())
                .field("aggregateid", payloadIdSchemaType)
                .field("type", eventTypeSchemaType)
                .field("payload", payloadSchemaType)
                .field("is_deleted", SchemaBuilder.bool().optional());

        extraFields.forEach(schemaBuilder::field);

        final Schema recordSchema = schemaBuilder.build();

        Envelope envelope = Envelope.defineSchema()
                .withName("event.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();

        final Struct after = new Struct(recordSchema);
        after.put("id", eventId);
        after.put("aggregatetype", payloadType);
        after.put("aggregateid", payloadId);
        after.put("type", eventType);
        after.put("payload", payload);

        extraValues.forEach(after::put);

        final Struct body = envelope.create(after, null, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), body);
    }

    private Struct asStruct(Object object) {
        assertThat(object).isInstanceOf(Struct.class);
        return (Struct) object;
    }
}
