/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.HeaderFrom;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIsNot;
import io.debezium.connector.postgresql.transforms.DecodeLogicalDecodingMessageContent;
import io.debezium.converters.AbstractCloudEventsConverterTest;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.transforms.outbox.EventRouter;

/**
 * Integration test for {@link io.debezium.converters.CloudEventsConverter} with {@link PostgresConnector}
 *
 * @author Roman Kudryashov
 */
public class CloudEventsConverterIT extends AbstractCloudEventsConverterTest<PostgresConnector> {

    private static final String SETUP_SCHEMA = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "CREATE SCHEMA s1;";

    private static final String SETUP_TABLE = "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));";

    private static final String SETUP_OUTBOX_SCHEMA = "DROP SCHEMA IF EXISTS outboxsmtit CASCADE;" +
            "CREATE SCHEMA outboxsmtit;";

    private static final String SETUP_OUTBOX_TABLE = "CREATE TABLE outboxsmtit.outbox (" +
            "  id                   uuid         not null   constraint outbox_pk primary key," +
            "  aggregatetype        varchar(255) not null," +
            "  aggregateid          varchar(255) not null," +
            "  event_type           varchar(255) not null," +
            "  tracingspancontext   varchar(255)," +
            "  payload              jsonb" +
            ");";

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);";

    @Rule
    public final TestRule skipName = new SkipTestDependingOnDecoderPluginNameRule();

    @Before
    @Override
    public void beforeEach() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
        super.beforeEach();
    }

    @Override
    protected Class<PostgresConnector> getConnectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected String getConnectorName() {
        return "postgresql";
    }

    @Override
    protected String getServerName() {
        return TestHelper.TEST_SERVER;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return TestHelper.create();
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "outboxsmtit,s1")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "outboxsmtit.outbox,s1.a");
    }

    @Override
    protected String topicName() {
        return TestHelper.topicName("s1.a");
    }

    @Override
    protected String topicNameOutbox() {
        return TestHelper.topicName("outboxsmtit.outbox");
    }

    protected String topicNameMessage() {
        return TestHelper.topicName("message");
    }

    @Override
    protected void createTable() throws Exception {
        TestHelper.execute(SETUP_SCHEMA);
        TestHelper.execute(SETUP_TABLE);
    }

    @Override
    protected void createOutboxTable() throws Exception {
        TestHelper.execute(SETUP_OUTBOX_SCHEMA);
        TestHelper.execute(SETUP_OUTBOX_TABLE);
    }

    @Override
    protected String createInsert() {
        return INSERT_STMT;
    }

    @Override
    protected String createInsertToOutbox(String eventId,
                                          String eventType,
                                          String aggregateType,
                                          String aggregateId,
                                          String tracingSpanContext,
                                          String payloadJson,
                                          String additional) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO outboxsmtit.outbox VALUES (");
        insert.append("'").append(UUID.fromString(eventId)).append("'");
        insert.append(", '").append(aggregateType).append("'");
        insert.append(", '").append(aggregateId).append("'");
        insert.append(", '").append(eventType).append("'");

        if (tracingSpanContext == null) {
            insert.append(", null");
        }
        else {
            insert.append(", '").append(tracingSpanContext).append("'");
        }
        if (payloadJson == null) {
            insert.append(", null::jsonb");
        }
        else if (payloadJson.isEmpty()) {
            insert.append(", ''");
        }
        else {
            insert.append(", '").append(payloadJson).append("'::jsonb");
        }

        if (additional != null) {
            insert.append(additional);
        }
        insert.append(")");

        return insert.toString();
    }

    protected String createStatementToCallInsertToWalFunction(String eventId,
                                                              String eventType,
                                                              String aggregateType,
                                                              String aggregateId,
                                                              String payloadJson) {
        StringBuilder statement = new StringBuilder();
        statement.append("SELECT pg_logical_emit_message(true, 'foo', '");

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.put("id", eventId);
        rootNode.put("type", eventType);
        rootNode.put("aggregateType", aggregateType);
        rootNode.put("aggregateId", aggregateId);
        rootNode.put("payload", payloadJson);

        statement.append(rootNode).append("');");

        return statement.toString();
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }

    @Test
    @FixFor("DBZ-8103")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Only supported on PgOutput")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, minor = 0, reason = "Message not supported for PG version < 14")
    public void shouldConvertToCloudEventsInJsonWithDataAsJsonAndAllMetadataInHeadersAfterOutboxEventRouterAppliedToLogicalDecodingMessage() throws Exception {
        HeaderFrom<SourceRecord> headerFrom = new HeaderFrom.Value<>();
        Map<String, String> headerFromConfig = new LinkedHashMap<>();
        headerFromConfig.put("fields", "source,op");
        headerFromConfig.put("headers", "source,op");
        headerFromConfig.put("operation", "copy");
        headerFromConfig.put("header.converter.schemas.enable", "true");
        headerFrom.configure(headerFromConfig);

        DecodeLogicalDecodingMessageContent<SourceRecord> decodeLogicalDecodingMessageContent = new DecodeLogicalDecodingMessageContent<>();
        Map<String, String> smtConfig = new LinkedHashMap<>();
        decodeLogicalDecodingMessageContent.configure(smtConfig);

        EventRouter<SourceRecord> outboxEventRouter = new EventRouter<>();
        Map<String, String> outboxEventRouterConfig = new LinkedHashMap<>();
        outboxEventRouterConfig.put("table.expand.json.payload", "true");
        outboxEventRouterConfig.put("table.field.event.key", "aggregateId");
        // this adds `type` header with value from the DB column. `id` header is added by Outbox Event Router by default
        outboxEventRouterConfig.put("table.fields.additional.placement", "type:header");
        outboxEventRouterConfig.put("route.by.field", "aggregateType");
        outboxEventRouter.configure(outboxEventRouterConfig);

        // emit non transactional logical decoding message
        TestHelper.execute(createStatementToCallInsertToWalFunction("59a42efd-b015-44a9-9dde-cb36d9002425",
                "UserCreated",
                "User",
                "10711fa5",
                "{" +
                        "\"someField1\": \"some value 1\"," +
                        "\"someField2\": 7005" +
                        "}"));

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic(topicNameMessage()).get(0);
        SourceRecord recordWithMetadataHeaders = headerFrom.apply(record);
        SourceRecord recordWithDecodedLogicalDecodingMessageContent = decodeLogicalDecodingMessageContent.apply(recordWithMetadataHeaders);
        SourceRecord routedEvent = outboxEventRouter.apply(recordWithDecodedLogicalDecodingMessageContent);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.User");
        assertThat(routedEvent.keySchema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        assertThat(routedEvent.key()).isEqualTo("10711fa5");
        assertThat(routedEvent.value()).isInstanceOf(Struct.class);

        CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithMetadataAndIdAndTypeInHeaders(routedEvent, getConnectorName(), getServerName());

        headerFrom.close();
        decodeLogicalDecodingMessageContent.close();
        outboxEventRouter.close();
    }
}
