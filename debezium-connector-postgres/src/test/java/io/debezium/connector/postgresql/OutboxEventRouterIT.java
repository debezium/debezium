/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.fest.assertions.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.data.Uuid;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.transforms.outbox.EventRouter;

/**
 * Integration test for {@link io.debezium.transforms.outbox.EventRouter} with {@link PostgresConnector}
 *
 * @author Renato Mefi (gh@mefi.in)
 */
public class OutboxEventRouterIT extends AbstractConnectorTest {

    private static final String SETUP_OUTBOX_SCHEMA = "DROP SCHEMA IF EXISTS outboxsmtit CASCADE;" +
            "CREATE SCHEMA outboxsmtit;";

    private static final String SETUP_OUTOBOX_TABLE = "CREATE TABLE outboxsmtit.outbox " +
            "(" +
            "  id            uuid         not null" +
            "    constraint outbox_pk primary key," +
            "  aggregatetype varchar(255) not null," +
            "  aggregateid   varchar(255) not null," +
            "  type          varchar(255) not null," +
            "  payload       jsonb        not null" +
            ");";

    private EventRouter<SourceRecord> outboxEventRouter;

    private static String createEventInsert(
            UUID eventId,
            String eventType,
            String aggregateType,
            String aggregateId,
            String payloadJson,
            String additional
    ) {
        return String.format("INSERT INTO outboxsmtit.outbox VALUES (" +
                        "'%s'" +
                        ", '%s'" +
                        ", '%s'" +
                        ", '%s'" +
                        ", '%s'::jsonb" +
                        "%s" +
                        ");",
                eventId,
                aggregateType,
                aggregateId,
                eventType,
                payloadJson,
                additional);
    }

    @Before
    public void beforeEach() {
        outboxEventRouter = new EventRouter<>();
        outboxEventRouter.configure(Collections.emptyMap());

        TestHelper.execute(SETUP_OUTBOX_SCHEMA);
        TestHelper.execute(SETUP_OUTOBOX_TABLE);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_WHITELIST, "outboxsmtit")
                .with(PostgresConnectorConfig.TABLE_WHITELIST, "outboxsmtit\\.outbox");
        start(PostgresConnector.class, configBuilder.build());

        assertConnectorIsRunning();
    }

    @After
    public void afterEach() {
        stopConnector();
        assertNoRecordsToConsume();
        outboxEventRouter.close();
    }

    @Test
    public void shouldConsumeRecordsFromInsert() throws Exception {
        TestHelper.execute(createEventInsert(
                UUID.fromString("59a42efd-b015-44a9-9dde-cb36d9002425"),
                "UserCreated",
                "User",
                "10711fa5",
                "{}",
                ""
        ));

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName("outboxsmtit.outbox")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(newEventRecord);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.user");

        Struct valueStruct = requireStruct(routedEvent.value(), "test payload");
        assertThat(valueStruct.getString("eventType")).isEqualTo("UserCreated");
        JsonNode payload = (new ObjectMapper()).readTree(valueStruct.getString("payload"));
        assertThat(payload.get("email")).isEqualTo(null);

    }

    @Test
    public void shouldRespectJsonFormatAsString() throws Exception {
        TestHelper.execute(createEventInsert(
                UUID.fromString("f9171eb6-19f3-4579-9206-0e179d2ebad7"),
                "UserCreated",
                "User",
                "7bdf2e9e",
                "{\"email\": \"gh@mefi.in\"}",
                ""
        ));

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName("outboxsmtit.outbox")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(newEventRecord);

        Struct valueStruct = requireStruct(routedEvent.value(), "test payload");
        assertThat(valueStruct.getString("eventType")).isEqualTo("UserCreated");
        JsonNode payload = (new ObjectMapper()).readTree(valueStruct.getString("payload"));
        assertThat(payload.get("email").getTextValue()).isEqualTo("gh@mefi.in");
    }

    @Test
    public void shouldSupportAllFeatures() throws Exception {
        outboxEventRouter = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put("table.field.event.schema.version", "version");
        config.put("table.field.event.timestamp", "createdat");
        config.put(
                "table.fields.additional.placement",
                "version:envelope:eventVersion," +
                        "aggregatetype:envelope:aggregateType," +
                        "somebooltype:envelope:someBoolType," +
                        "somebooltype:header"
        );
        outboxEventRouter.configure(config);

        TestHelper.execute("ALTER TABLE outboxsmtit.outbox add version int not null;");
        TestHelper.execute("ALTER TABLE outboxsmtit.outbox add somebooltype boolean not null;");
        TestHelper.execute("ALTER TABLE outboxsmtit.outbox add createdat timestamp without time zone not null;");

        TestHelper.execute(createEventInsert(
                UUID.fromString("f9171eb6-19f3-4579-9206-0e179d2ebad7"),
                "UserUpdated",
                "UserEmail",
                "7bdf2e9e",
                "{\"email\": \"gh@mefi.in\"}",
                ", 1, true, TIMESTAMP '2019-03-24 20:52:59'"
        ));

        SourceRecords actualRecords = consumeRecordsByTopic(1);
        assertThat(actualRecords.topics().size()).isEqualTo(1);

        SourceRecord newEventRecord = actualRecords.recordsForTopic(topicName("outboxsmtit.outbox")).get(0);
        SourceRecord eventRouted = outboxEventRouter.apply(newEventRecord);

        // Validate metadata
        assertThat(eventRouted.valueSchema().version()).isEqualTo(1);
        assertThat(eventRouted.timestamp()).isEqualTo(1553460779000000L);
        assertThat(eventRouted.topic()).isEqualTo("outbox.event.useremail");

        // Validate headers
        Headers headers = eventRouted.headers();
        assertThat(headers.size()).isEqualTo(2);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(Uuid.schema());
        assertThat(headerId.value()).isEqualTo("f9171eb6-19f3-4579-9206-0e179d2ebad7");
        Header headerBool = headers.lastWithName("somebooltype");
        assertThat(headerBool.schema()).isEqualTo(SchemaBuilder.BOOLEAN_SCHEMA);
        assertThat(headerBool.value()).isEqualTo(true);

        // Validate Key
        assertThat(eventRouted.keySchema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(eventRouted.key()).isEqualTo("7bdf2e9e");

        // Validate message body
        Struct valueStruct = requireStruct(eventRouted.value(), "test envelope");
        assertThat(valueStruct.getString("eventType")).isEqualTo("UserUpdated");
        assertThat(valueStruct.getString("aggregateType")).isEqualTo("UserEmail");
        assertThat(valueStruct.getInt32("eventVersion")).isEqualTo(1);
        assertThat(valueStruct.getBoolean("someBoolType")).isEqualTo(true);
    }
}
