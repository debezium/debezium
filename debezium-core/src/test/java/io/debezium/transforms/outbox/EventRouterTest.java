/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Unit tests for {@link EventRouter}
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouterTest {

    @Test
    public void canSkipTombstone() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.outbox",
                null,
                null
        );
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
        final Struct payload = envelope.delete(before, null, System.nanoTime());
        final SourceRecord eventRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), payload);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNull();
    }

    @Test
    public void canExtractTableFields() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        router.configure(config);

        final SourceRecord eventRecord = createEventRecord();
        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(((Struct) eventRouted.value()).getString("payload")).isEqualTo("{}");
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
    public void canConfigureEveryTableField() {
        final EventRouter<SourceRecord> router = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_EVENT_ID.name(), "event_id");
        config.put(EventRouterConfigDefinition.FIELD_PAYLOAD_TYPE.name(), "payload_type");
        config.put(EventRouterConfigDefinition.FIELD_PAYLOAD_ID.name(), "payload_id");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TYPE.name(), "event_type");
        config.put(EventRouterConfigDefinition.FIELD_PAYLOAD.name(), "payload_body");
        router.configure(config);

        final Schema recordSchema = SchemaBuilder.struct()
                .field("event_id", SchemaBuilder.string())
                .field("payload_type", SchemaBuilder.string())
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
        before.put("payload_type", "User");
        before.put("payload_id", "10711fa5");
        before.put("event_type", "UserCreated");
        before.put("payload_body", "{}");

        final Struct payload = envelope.create(before, null, System.nanoTime());
        final SourceRecord eventRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), payload);

        final SourceRecord eventRouted = router.apply(eventRecord);

        assertThat(eventRouted).isNotNull();
        assertThat(((Struct) eventRouted.value()).getString("payload")).isEqualTo("{}");
    }

    private SourceRecord createEventRecord() {
        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.string())
                .field("aggregatetype", SchemaBuilder.string())
                .field("aggregateid", SchemaBuilder.string())
                .field("type", SchemaBuilder.string())
                .field("payload", SchemaBuilder.string())
                .build();

        Envelope envelope = Envelope.defineSchema()
                .withName("event.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();

        final Struct before = new Struct(recordSchema);
        before.put("id", "da8d6de6-3b77-45ff-8f44-57db55a7a06c");
        before.put("aggregatetype", "User");
        before.put("aggregateid", "10711fa5");
        before.put("type", "UserCreated");
        before.put("payload", "{}");

        final Struct payload = envelope.create(before, null, System.nanoTime());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "db.outbox", envelope.schema(), payload);
    }
}
