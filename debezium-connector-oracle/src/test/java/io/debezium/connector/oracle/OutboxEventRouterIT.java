/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.data.VerifyRecord.assertConnectSchemasAreEqual;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.fest.assertions.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.converters.NumberOneToBooleanConverter;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.transforms.outbox.EventRouter;
import io.debezium.transforms.outbox.EventRouterConfigDefinition;
import io.debezium.util.Testing;

/**
 * An integration test for Oracle and the {@link EventRouter} for outbox.
 *
 * @author Chris Cranford
 */
public class OutboxEventRouterIT extends AbstractConnectorTest {

    private static final String SETUP_OUTBOX_TABLE = "CREATE TABLE debezium.outbox (" +
            "id varchar2(64) not null primary key, " +
            "aggregatetype varchar2(255) not null, " +
            "aggregateid varchar2(255) not null, " +
            "type varchar2(255) not null, " +
            "payload varchar2(4000))";

    private EventRouter<SourceRecord> outboxEventRouter;
    private OracleConnection connection;

    @Before
    public void before() throws Exception {
        connection = TestHelper.testConnection();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);

        TestHelper.dropTable(connection, "debezium.outbox");

        connection.execute(SETUP_OUTBOX_TABLE);
        TestHelper.streamTable(connection, "debezium.outbox");

        outboxEventRouter = new EventRouter<>();
        outboxEventRouter.configure(Collections.emptyMap()); // default values
    }

    @After
    public void after() throws Exception {
        stopConnector();
        assertNoRecordsToConsume();
        outboxEventRouter.close();

        if (connection != null && connection.isConnected()) {
            TestHelper.dropTable(connection, "debezium.outbox");
            connection.close();
        }
    }

    @Test
    @FixFor({"DBZ-1169", "DBZ-3940"})
    public void shouldConsumeRecordsFromInsert() throws Exception {
        startConnectorWithInitialSnapshotRecord();

        connection.execute(createEventInsert(
                "59a42efd-b015-44a9-9dde-cb36d9002425",
                "UserCreated",
                "User",
                "10711fa5",
                "{}",
                ""));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName("OUTBOX")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.User");

        assertThat(routedEvent.keySchema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(routedEvent.key()).isEqualTo("10711fa5");

        assertThat(routedEvent.value()).isInstanceOf(String.class);
        JsonNode payload = (new ObjectMapper()).readTree((String) routedEvent.value());
        assertThat(payload.get("email")).isNull();
    }

    @Test
    @FixFor({"DBZ-1385", "DBZ-3940"})
    public void shouldSendEventTypeAsHeader() throws Exception {
        startConnectorWithInitialSnapshotRecord();
        connection.execute(createEventInsert(
                "59a42efd-b015-44a9-9dde-cb36d9002425",
                "UserCreated",
                "User",
                "10711fa5",
                "{\"email\": \"gh@mefi.in\"}",
                ""));

        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "TYPE:header:eventType");
        outboxEventRouter.configure(config);

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName("OUTBOX")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.User");

        Object value = routedEvent.value();
        assertThat(routedEvent.headers().lastWithName("eventType").value()).isEqualTo("UserCreated");

        assertThat(value).isInstanceOf(String.class);
        JsonNode payload = (new ObjectMapper()).readTree((String) value);
        assertThat(payload.get("email").asText()).isEqualTo("gh@mefi.in");
    }

    @Test
    @FixFor({"DBZ-2014", "DBZ-3940"})
    public void shouldSendEventTypeAsValue() throws Exception {
        startConnectorWithInitialSnapshotRecord();
        connection.execute(createEventInsert(
                "d4da2428-8b19-11ea-bc55-0242ac130003",
                "UserCreated",
                "User",
                "9948fcad",
                "{\"email\": \"gh@mefi.in\"}",
                ""));

        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), "TYPE:envelope:eventType");
        outboxEventRouter.configure(config);

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName("OUTBOX")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.User");

        Struct valueStruct = requireStruct(routedEvent.value(), "test payload");
        assertThat(valueStruct.getString("eventType")).isEqualTo("UserCreated");
        JsonNode payload = (new ObjectMapper()).readTree(valueStruct.getString("payload"));
        assertThat(payload.get("email").asText()).isEqualTo("gh@mefi.in");
    }

    @Test
    @FixFor({"DBZ-2014", "DBZ-3940"})
    public void shouldRespectJsonFormatAsString() throws Exception {
        startConnectorWithInitialSnapshotRecord();
        connection.execute(createEventInsert(
                "f9171eb6-19f3-4579-9206-0e179d2ebad7",
                "UserCreated",
                "User",
                "7bdf2e9e",
                "{\"email\": \"gh@mefi.in\"}",
                ""));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName("OUTBOX")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        assertThat(routedEvent.value()).isInstanceOf(String.class);
        JsonNode payload = (new ObjectMapper()).readTree((String) routedEvent.value());
        assertThat(payload.get("email").asText()).isEqualTo("gh@mefi.in");
    }

    @Test
    @FixFor({"DBZ-1169", "DBZ-3940"})
    public void shouldSupportAllFeatures() throws Exception {
        startConnectorWithNoSnapshot();

        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), "VERSION");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), "CREATEDAT");
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                   "VERSION:envelope:eventVersion," +
                "AGGREGATETYPE:envelope:aggregateType," +
                "SOMEBOOLTYPE:envelope:someBoolType," +
                "SOMEBOOLTYPE:header," +
                "IS_DELETED:envelope:deleted");
        outboxEventRouter.configure(config);

        connection.execute("ALTER TABLE debezium.outbox add version numeric(9,0) not null");
        connection.execute("ALTER TABLE debezium.outbox add somebooltype numeric(1,0) not null");
        connection.execute("ALTER TABLE debezium.outbox add createdat timestamp not null");
        connection.execute("ALTER TABLE debezium.outbox add is_deleted numeric(1,0) default 0 not null");

        connection.execute(createEventInsert(
                "f9171eb6-19f3-4579-9206-0e179d2ebad7",
                "UserUpdated",
                "UserEmail",
                "7bdf2e9e",
                "{\"email\": \"gh@mefi.in\"}",
                ", 1, 1, TO_TIMESTAMP('2019-03-24 20:52:59', 'YYYY-MM-DD HH24:MI:SS'), 0"));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName("OUTBOX")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // Validate metadata
        Schema expectedSchema = SchemaBuilder.struct()
                .version(1)
                .name("server1.DEBEZIUM.OUTBOX.UserEmail.Value")
                .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
                .field("eventVersion", Schema.INT32_SCHEMA)
                .field("aggregateType", Schema.STRING_SCHEMA)
                .field("someBoolType", Schema.BOOLEAN_SCHEMA)
                .field("deleted", SchemaBuilder.bool().defaultValue(false).build())
                .build();

        assertConnectSchemasAreEqual(null, routedEvent.valueSchema(), expectedSchema);
        assertThat(routedEvent.timestamp()).isEqualTo(1553460779000L);
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.UserEmail");

        // Validate headers
        Headers headers = routedEvent.headers();
        assertThat(headers).hasSize(2);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(headerId.value()).isEqualTo("f9171eb6-19f3-4579-9206-0e179d2ebad7");
        Header headerBool = headers.lastWithName("SOMEBOOLTYPE");
        assertThat(headerBool.schema()).isEqualTo(SchemaBuilder.BOOLEAN_SCHEMA);
        assertThat(headerBool.value()).isEqualTo(true);

        // Validate Key
        assertThat(routedEvent.keySchema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(routedEvent.key()).isEqualTo("7bdf2e9e");

        // Validate message body
        Struct valueStruct = requireStruct(routedEvent.value(), "test envelope");
        assertThat(valueStruct.getString("aggregateType")).isEqualTo("UserEmail");
        assertThat(valueStruct.getInt32("eventVersion")).isEqualTo(1);
        assertThat(valueStruct.get("someBoolType")).isEqualTo(true);
        assertThat(valueStruct.get("deleted")).isEqualTo(false);
    }
    
    @Test
    @FixFor({"DBZ-1707", "DBZ-3940"})
    public void shouldConvertMicroSecondsTimestampToMilliSeconds() throws Exception {
        startConnectorWithNoSnapshot();

        outboxEventRouter = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), "CREATEDAT");
        outboxEventRouter.configure(config);

        connection.execute("ALTER TABLE debezium.outbox add createdat timestamp not null");
        connection.execute(createEventInsert(
                "f9171eb6-19f3-4579-9206-0e179d2ebad7",
                "UserUpdated",
                "UserEmail",
                "7bdf2e9e",
                "{\"email\": \"gh@mefi.in\"}",
                ", TO_TIMESTAMP('2019-03-24 20:52:59', 'YYYY-MM-DD HH24:MI:SS')"));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName("OUTBOX")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // expecting microseconds value emitted for TIMESTAMP column without width to be
        // converted to milliseconds, as that's the standard semantics of that property
        // in Kafka
        assertThat(routedEvent.timestamp()).isEqualTo(1553460779000L);
    }

    @Test
    @FixFor({"DBZ-1320", "DBZ-3940"})
    public void shouldNotProduceTombstoneEventForNullPayload() throws Exception {
        startConnectorWithNoSnapshot();

        outboxEventRouter = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), "VERSION");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), "CREATEDAT");
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "VERSION:envelope:eventVersion," +
                        "AGGREGATETYPE:envelope:aggregateType," +
                        "SOMEBOOLTYPE:envelope:someBoolType," +
                        "SOMEBOOLTYPE:header," +
                        "IS_DELETED:envelope:deleted");
        outboxEventRouter.configure(config);

        connection.execute("ALTER TABLE debezium.outbox add version numeric(9,0) not null");
        connection.execute("ALTER TABLE debezium.outbox add somebooltype numeric(1,0) not null");
        connection.execute("ALTER TABLE debezium.outbox add createdat timestamp not null");
        connection.execute("ALTER TABLE debezium.outbox add is_deleted numeric(1,0) default 0 not null");

        connection.execute(createEventInsert(
                "a9d76f78-bda6-48d3-97ed-13a146163218",
                "UserUpdated",
                "UserEmail",
                "a9d76f78",
                null,
                ", 1, 1, TO_TIMESTAMP('2019-03-24 20:52:59', 'YYYY-MM-DD HH24:MI:SS'), 1"));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.topics()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName("OUTBOX")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // Validate metadata
        assertThat(routedEvent.valueSchema()).isNotNull();
        assertThat(routedEvent.timestamp()).isEqualTo(1553460779000L);
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.UserEmail");

        // Validate headers
        Headers headers = routedEvent.headers();
        assertThat(headers.size()).isEqualTo(2);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(headerId.value()).isEqualTo("a9d76f78-bda6-48d3-97ed-13a146163218");
        Header headerBool = headers.lastWithName("SOMEBOOLTYPE");
        assertThat(headerBool.schema()).isEqualTo(SchemaBuilder.BOOLEAN_SCHEMA);
        assertThat(headerBool.value()).isEqualTo(true);

        // Validate Key
        assertThat(routedEvent.keySchema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(routedEvent.key()).isEqualTo("a9d76f78");

        // Validate message body
        System.out.println(routedEvent);
        assertThat(routedEvent.value()).isNotNull();
        assertThat(((Struct) routedEvent.value()).get("payload")).isNull();
    }
    
    @Test
    @FixFor({"DBZ-1320", "DBZ-3940"})
    public void shouldProduceTombstoneEventForNullPayload() throws Exception {
        startConnectorWithNoSnapshot();

        outboxEventRouter = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), "VERSION");
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), "CREATEDAT");
        config.put(EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name(), "true");
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(),
                "VERSION:envelope:eventVersion," +
                        "AGGREGATETYPE:envelope:aggregateType," +
                        "SOMEBOOLTYPE:envelope:someBoolType," +
                        "SOMEBOOLTYPE:header," +
                        "IS_DELETED:envelope:deleted");
        outboxEventRouter.configure(config);

        connection.execute("ALTER TABLE debezium.outbox add version numeric(9,0) not null");
        connection.execute("ALTER TABLE debezium.outbox add somebooltype numeric(1,0) not null");
        connection.execute("ALTER TABLE debezium.outbox add createdat timestamp not null");
        connection.execute("ALTER TABLE debezium.outbox add is_deleted numeric(1,0) default 0 not null");

        connection.execute(createEventInsert(
                "a9d76f78-bda6-48d3-97ed-13a146163218",
                "UserUpdated",
                "UserEmail",
                "a9d76f78",
                null,
                ", 1, 1, TO_TIMESTAMP('2019-03-24 20:52:59', 'YYYY-MM-DD HH24:MI:SS'), 1"));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.topics()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName("OUTBOX")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // Validate metadata
        assertThat(routedEvent.valueSchema()).isNull();
        assertThat(routedEvent.timestamp()).isEqualTo(1553460779000L);
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.UserEmail");

        // Validate headers
        Headers headers = routedEvent.headers();
        assertThat(headers.size()).isEqualTo(2);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(headerId.value()).isEqualTo("a9d76f78-bda6-48d3-97ed-13a146163218");
        Header headerBool = headers.lastWithName("SOMEBOOLTYPE");
        assertThat(headerBool.schema()).isEqualTo(SchemaBuilder.BOOLEAN_SCHEMA);
        assertThat(headerBool.value()).isEqualTo(true);

        // Validate Key
        assertThat(routedEvent.keySchema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(routedEvent.key()).isEqualTo("a9d76f78");

        // Validate message body
        assertThat(routedEvent.value()).isNull();
    }

    @Test
    @FixFor({"DBZ-1320", "DBZ-3940"})
    public void shouldProduceTombstoneEventForEmptyPayload() throws Exception {
        startConnectorWithNoSnapshot();

        outboxEventRouter = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name(), "true");
        outboxEventRouter.configure(config);

        connection.execute("ALTER TABLE debezium.outbox modify (payload varchar2(1000))");
        connection.execute(createEventInsert(
                "a9d76f78-bda6-48d3-97ed-13a146163218",
                "UserUpdated",
                "UserEmail",
                "a9d76f78",
                "",
                null));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.topics()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName("OUTBOX")).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // Validate metadata
        assertThat(routedEvent.valueSchema()).isNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.UserEmail");

        // Validate headers
        Headers headers = routedEvent.headers();
        assertThat(headers.size()).isEqualTo(1);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(headerId.value()).isEqualTo("a9d76f78-bda6-48d3-97ed-13a146163218");

        // Validate Key
        assertThat(routedEvent.keySchema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(routedEvent.key()).isEqualTo("a9d76f78");

        // Validate message body
        assertThat(routedEvent.value()).isNull();
    }    

    private void startConnectorWithInitialSnapshotRecord() throws Exception {
        connection.execute(createEventInsert(
                "70f52ae3-f671-4bac-ae62-1b9be6e73700",
                "UserCreated",
                "User",
                "10711faf",
                "{}",
                ""));

        Configuration.Builder configBuilder = getConfigurationBuilder(SnapshotMode.INITIAL);
        start(OracleConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.allRecordsInOrder()).hasSize(1);

        List<SourceRecord> records = snapshotRecords.recordsForTopic(topicName("OUTBOX"));
        assertThat(records).hasSize(1);

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    private void startConnectorWithNoSnapshot() throws Exception {
        Configuration.Builder configBuilder = getConfigurationBuilder(SnapshotMode.SCHEMA_ONLY);
        start(OracleConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        assertNoRecordsToConsume();
    }

    private Configuration.Builder getConfigurationBuilder(SnapshotMode snapshotMode) {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, snapshotMode.getValue())
                // this allows numeric(1) to be simulated as boolean types like other databases
                .with(OracleConnectorConfig.CUSTOM_CONVERTERS, "boolean")
                .with("boolean.type", NumberOneToBooleanConverter.class.getName())
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.OUTBOX");
    }

    private String topicName(String tableName) {
        return TestHelper.SERVER_NAME + ".DEBEZIUM." + tableName;
    }

    private String createEventInsert(String id, String type, String aggregateType, String aggregateId, String payload, String additional) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO debezium.outbox VALUES (");
        insert.append("'").append(id).append("', ");
        insert.append("'").append(aggregateType).append("', ");
        insert.append("'").append(aggregateId).append("', ");
        insert.append("'").append(type).append("', ");
        if (payload != null) {
            insert.append("'").append(payload).append("'");
        }
        else {
            insert.append("NULL");
        }
        if (additional != null) {
            insert.append(additional);
        }
        insert.append(")");
        return insert.toString();
    }
}
