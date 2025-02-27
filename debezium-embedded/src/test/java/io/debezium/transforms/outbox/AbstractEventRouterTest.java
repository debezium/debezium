/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import static io.debezium.data.VerifyRecord.assertConnectSchemasAreEqual;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;

/**
 * A unified test of all {@link EventRouter} behavior which all connectors should extend.
 *
 * @author Chris Cranford
 */
public abstract class AbstractEventRouterTest<T extends SourceConnector> extends AbstractAsyncEngineConnectorTest {

    protected EventRouter<SourceRecord> outboxEventRouter;

    protected abstract Class<T> getConnectorClass();

    protected abstract JdbcConnection databaseConnection();

    protected abstract Configuration.Builder getConfigurationBuilder(boolean initialSnapshot);

    protected abstract String topicName();

    protected abstract String tableName();

    protected abstract String getSchemaNamePrefix();

    protected abstract Schema getPayloadSchema();

    protected abstract void createTable() throws Exception;

    protected abstract void alterTableWithExtra4Fields() throws Exception;

    protected abstract void alterTableWithTimestampField() throws Exception;

    protected abstract void alterTableModifyPayload() throws Exception;

    protected abstract String getAdditionalFieldValues(boolean deleted);

    protected abstract String getAdditionalFieldValuesTimestampOnly();

    protected abstract String createInsert(String eventId, String eventType, String aggregateType,
                                           String aggregateId, String payloadJson, String additional);

    protected abstract void waitForSnapshotCompleted() throws InterruptedException;

    protected abstract void waitForStreamingStarted() throws InterruptedException;

    @Before
    public void beforeEach() throws Exception {
        createTable();
        outboxEventRouter = new EventRouter<>();
        outboxEventRouter.configure(Collections.emptyMap()); // configure with defaults
    }

    @After
    public void afterEach() throws Exception {
        stopConnector();
        assertNoRecordsToConsume();
        outboxEventRouter.close();
    }

    @Test
    @FixFor({ "DBZ-1169", "DBZ-3940" })
    public void shouldConsumeRecordsFromInsert() throws Exception {
        startConnectorWithInitialSnapshotRecord();
        databaseConnection().execute(createInsert(
                "59a42efd-b015-44a9-9dde-cb36d9002425",
                "UserCreated",
                "User",
                "10711fa5",
                "{}",
                ""));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
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
    @FixFor({ "DBZ-1385", "DBZ-3940" })
    public void shouldSendEventTypeAsHeader() throws Exception {
        startConnectorWithInitialSnapshotRecord();
        databaseConnection().execute(createInsert(
                "59a42efd-b015-44a9-9dde-cb36d9002425",
                "UserCreated",
                "User",
                "10711fa5",
                "{\"email\": \"gh@mefi.in\"}",
                ""));

        final Map<String, String> config = new HashMap<>();
        final String placements = getFieldEventType() + ":header:eventType";
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), placements);
        outboxEventRouter.configure(config);

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
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
    @FixFor({ "DBZ-2014", "DBZ-3940" })
    public void shouldSendEventTypeAsValue() throws Exception {
        startConnectorWithInitialSnapshotRecord();
        databaseConnection().execute(createInsert(
                "d4da2428-8b19-11ea-bc55-0242ac130003",
                "UserCreated",
                "User",
                "9948fcad",
                "{\"email\": \"gh@mefi.in\"}",
                ""));

        final Map<String, String> config = new HashMap<>();
        final String placements = getFieldEventType() + ":envelope:eventType";
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), placements);
        outboxEventRouter.configure(config);

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.User");

        Struct valueStruct = requireStruct(routedEvent.value(), "test payload");
        assertThat(valueStruct.getString("eventType")).isEqualTo("UserCreated");
        JsonNode payload = (new ObjectMapper()).readTree(valueStruct.getString("payload"));
        assertThat(payload.get("email").asText()).isEqualTo("gh@mefi.in");
    }

    @Test
    @FixFor({ "DBZ-2014", "DBZ-3940" })
    public void shouldRespectJsonFormatAsString() throws Exception {
        startConnectorWithInitialSnapshotRecord();
        databaseConnection().execute(createInsert(
                "f9171eb6-19f3-4579-9206-0e179d2ebad7",
                "UserCreated",
                "User",
                "7bdf2e9e",
                "{\"email\": \"gh@mefi.in\"}",
                ""));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        assertThat(routedEvent.value()).isInstanceOf(String.class);
        JsonNode payload = (new ObjectMapper()).readTree((String) routedEvent.value());
        assertThat(payload.get("email").asText()).isEqualTo("gh@mefi.in");
    }

    @Test
    @FixFor({ "DBZ-1169", "DBZ-3940" })
    public void shouldSupportAllFeatures() throws Exception {

        final StringBuilder placements = new StringBuilder();
        placements.append(envelope(getFieldSchemaVersion(), "eventVersion")).append(",");
        placements.append(envelope(getFieldAggregateType(), "aggregateType")).append(",");
        placements.append(envelope(getSomeBoolType(), "someBoolType")).append(",");
        placements.append(header(getSomeBoolType(), null)).append(",");
        placements.append(envelope(getIsDeleted(), "deleted"));

        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), getFieldSchemaVersion());
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), getFieldEventTimestamp());
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), placements.toString());
        outboxEventRouter.configure(config);

        alterTableWithExtra4Fields();

        startConnectorWithNoSnapshot();

        databaseConnection().execute(createInsert(
                "f9171eb6-19f3-4579-9206-0e179d2ebad7",
                "UserUpdated",
                "UserEmail",
                "7bdf2e9e",
                "{\"email\": \"gh@mefi.in\"}",
                getAdditionalFieldValues(false)));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // Validate metadata
        Schema expectedSchema = SchemaBuilder.struct()
                .version(1)
                .name(getSchemaNamePrefix() + "UserEmail.Value")
                .field("payload", getPayloadSchema())
                .field("eventVersion", Schema.INT32_SCHEMA)
                .field("aggregateType", Schema.STRING_SCHEMA)
                .field("someBoolType", Schema.BOOLEAN_SCHEMA)
                .field("deleted", SchemaBuilder.bool().optional().defaultValue(false).build())
                .build();

        assertConnectSchemasAreEqual(null, routedEvent.valueSchema(), expectedSchema);
        assertThat(routedEvent.timestamp()).isEqualTo(1553460779000L);
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.UserEmail");

        // Validate headers
        Headers headers = routedEvent.headers();
        assertThat(headers).hasSize(2);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(getIdSchema());
        assertThat(headerId.value()).isEqualTo(getId("f9171eb6-19f3-4579-9206-0e179d2ebad7"));
        Header headerBool = headers.lastWithName(getSomeBoolType());
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
    @FixFor({ "DBZ-1707", "DBZ-3940" })
    public void shouldConvertMicrosecondsTimestampToMilliseconds() throws Exception {

        outboxEventRouter = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), getFieldEventTimestamp());
        outboxEventRouter.configure(config);

        alterTableWithTimestampField();

        startConnectorWithNoSnapshot();

        databaseConnection().execute(createInsert(
                "f9171eb6-19f3-4579-9206-0e179d2ebad7",
                "UserUpdated",
                "UserEmail",
                "7bdf2e9e",
                "{\"email\": \"gh@mefi.in\"}",
                getAdditionalFieldValuesTimestampOnly()));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // expecting microseconds value emitted for TIMESTAMP column without width to be
        // converted to milliseconds, as that's the standard semantics of that property
        // in Kafka
        assertThat(routedEvent.timestamp()).isEqualTo(1553460779000L);
    }

    @Test
    @FixFor({ "DBZ-1320", "DBZ-3940" })
    public void shouldNotProduceTombstoneEventForNullPayload() throws Exception {

        final StringBuilder placements = new StringBuilder();
        placements.append(envelope(getFieldSchemaVersion(), "eventVersion")).append(",");
        placements.append(envelope(getFieldAggregateType(), "agregateType")).append(",");
        placements.append(envelope(getSomeBoolType(), "someBoolType")).append(",");
        placements.append(header(getSomeBoolType(), null)).append(",");
        placements.append(envelope(getIsDeleted(), "deleted"));

        outboxEventRouter = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), getFieldSchemaVersion());
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), getFieldEventTimestamp());
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), placements.toString());
        outboxEventRouter.configure(config);

        alterTableWithExtra4Fields();

        startConnectorWithNoSnapshot();

        databaseConnection().execute(createInsert(
                "a9d76f78-bda6-48d3-97ed-13a146163218",
                "UserUpdated",
                "UserEmail",
                "a9d76f78",
                null,
                getAdditionalFieldValues(true)));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.topics()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // Validate metadata
        assertThat(routedEvent.valueSchema()).isNotNull();
        assertThat(routedEvent.timestamp()).isEqualTo(1553460779000L);
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.UserEmail");

        // Validate headers
        Headers headers = routedEvent.headers();
        assertThat(headers.size()).isEqualTo(2);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(getIdSchema());
        assertThat(headerId.value()).isEqualTo(getId("a9d76f78-bda6-48d3-97ed-13a146163218"));
        Header headerBool = headers.lastWithName(getSomeBoolType());
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
    @FixFor({ "DBZ-1320", "DBZ-3940" })
    public void shouldProduceTombstoneEventForNullPayload() throws Exception {

        final StringBuilder placements = new StringBuilder();
        placements.append(envelope(getFieldSchemaVersion(), "eventVersion")).append(",");
        placements.append(envelope(getFieldAggregateType(), "aggregateType")).append(",");
        placements.append(envelope(getSomeBoolType(), "someBoolType")).append(",");
        placements.append(header(getSomeBoolType(), null)).append(",");
        placements.append(envelope(getIsDeleted(), "deleted"));

        outboxEventRouter = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.FIELD_SCHEMA_VERSION.name(), getFieldSchemaVersion());
        config.put(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP.name(), getFieldEventTimestamp());
        config.put(EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name(), "true");
        config.put(EventRouterConfigDefinition.FIELDS_ADDITIONAL_PLACEMENT.name(), placements.toString());
        outboxEventRouter.configure(config);

        alterTableWithExtra4Fields();

        startConnectorWithNoSnapshot();

        databaseConnection().execute(createInsert(
                "a9d76f78-bda6-48d3-97ed-13a146163218",
                "UserUpdated",
                "UserEmail",
                "a9d76f78",
                null,
                getAdditionalFieldValues(true)));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.topics()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // Validate metadata
        assertThat(routedEvent.valueSchema()).isNull();
        assertThat(routedEvent.timestamp()).isEqualTo(1553460779000L);
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.UserEmail");

        // Validate headers
        Headers headers = routedEvent.headers();
        assertThat(headers.size()).isEqualTo(2);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(getIdSchema());
        assertThat(headerId.value()).isEqualTo(getId("a9d76f78-bda6-48d3-97ed-13a146163218"));
        Header headerBool = headers.lastWithName(getSomeBoolType());
        assertThat(headerBool.schema()).isEqualTo(SchemaBuilder.BOOLEAN_SCHEMA);
        assertThat(headerBool.value()).isEqualTo(true);

        // Validate Key
        assertThat(routedEvent.keySchema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(routedEvent.key()).isEqualTo("a9d76f78");

        // Validate message body
        assertThat(routedEvent.value()).isNull();
    }

    @Test
    @FixFor({ "DBZ-1320", "DBZ-3940" })
    public void shouldProduceTombstoneEventForEmptyPayload() throws Exception {

        outboxEventRouter = new EventRouter<>();
        final Map<String, String> config = new HashMap<>();
        config.put(EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD.name(), "true");
        outboxEventRouter.configure(config);

        alterTableModifyPayload();

        startConnectorWithNoSnapshot();

        databaseConnection().execute(createInsert(
                "a9d76f78-bda6-48d3-97ed-13a146163218",
                "UserUpdated",
                "UserEmail",
                "a9d76f78",
                "",
                null));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.topics()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicName()).get(0);
        SourceRecord routedEvent = outboxEventRouter.apply(record);

        // Validate metadata
        assertThat(routedEvent.valueSchema()).isNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.UserEmail");

        // Validate headers
        Headers headers = routedEvent.headers();
        assertThat(headers.size()).isEqualTo(1);
        Header headerId = headers.lastWithName("id");
        assertThat(headerId.schema()).isEqualTo(getIdSchema());
        assertThat(headerId.value()).isEqualTo(getId("a9d76f78-bda6-48d3-97ed-13a146163218"));

        // Validate Key
        assertThat(routedEvent.keySchema()).isEqualTo(SchemaBuilder.STRING_SCHEMA);
        assertThat(routedEvent.key()).isEqualTo("a9d76f78");

        // Validate message body
        assertThat(routedEvent.value()).isNull();
    }

    protected String getFieldEventType() {
        return EventRouterConfigDefinition.FIELD_EVENT_TYPE.defaultValueAsString();
    }

    protected String getFieldSchemaVersion() {
        return "version";
    }

    protected String getFieldEventTimestamp() {
        return "createdat";
    }

    protected String getFieldAggregateType() {
        return "aggregatetype";
    }

    protected String getSomeBoolType() {
        return "somebooltype";
    }

    protected String getIsDeleted() {
        return "is_deleted";
    }

    protected Schema getIdSchema() {
        return SchemaBuilder.STRING_SCHEMA;
    }

    protected Object getId(String idValue) {
        return idValue;
    }

    private String envelope(String source, String destination) {
        return source + ":envelope:" + destination;
    }

    private String header(String source, String destination) {
        return source + ":header" + (destination != null && destination.length() > 0 ? ":" + destination : "");
    }

    private void startConnectorWithInitialSnapshotRecord() throws Exception {
        doInsert(createInsert("70f52ae3-f671-4bac-ae62-1b9be6e73700", "UserCreated", "User", "10711faf", "{}", ""));

        Configuration.Builder configBuilder = getConfigurationBuilder(true);
        start(getConnectorClass(), configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotCompleted();

        SourceRecords snapshotRecords = consumeRecordsByTopic(1);
        assertThat(snapshotRecords.allRecordsInOrder()).hasSize(1);

        List<SourceRecord> records = snapshotRecords.recordsForTopic(topicName());
        assertThat(records).hasSize(1);
    }

    private void startConnectorWithNoSnapshot() throws Exception {
        Configuration.Builder configBuilder = getConfigurationBuilder(false);
        start(getConnectorClass(), configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingStarted();
        assertNoRecordsToConsume();
    }

    private void doInsert(String insertSql) throws SQLException {
        databaseConnection().execute(insertSql);
    }
}
