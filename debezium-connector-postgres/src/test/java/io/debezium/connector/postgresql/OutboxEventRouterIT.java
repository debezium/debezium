/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.UUID;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.transforms.outbox.AbstractEventRouterTest;

/**
 * Integration test for {@link io.debezium.transforms.outbox.EventRouter} with {@link PostgresConnector}
 *
 * @author Renato Mefi (gh@mefi.in)
 */
public class OutboxEventRouterIT extends AbstractEventRouterTest<PostgresConnector> {

    private static final String SETUP_OUTBOX_SCHEMA = "DROP SCHEMA IF EXISTS outboxsmtit CASCADE;" +
            "CREATE SCHEMA outboxsmtit;";

    private static final String SETUP_OUTBOX_TABLE = "CREATE TABLE outboxsmtit.outbox " +
            "(" +
            "  id            uuid         not null" +
            "    constraint outbox_pk primary key," +
            "  aggregatetype varchar(255) not null," +
            "  aggregateid   varchar(255) not null," +
            "  type          varchar(255) not null," +
            "  payload       jsonb" +
            ");";

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
    protected JdbcConnection databaseConnection() {
        return TestHelper.create();
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder(boolean initialSnapshot) {
        SnapshotMode snapshotMode = initialSnapshot ? SnapshotMode.INITIAL : SnapshotMode.NO_DATA;
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, snapshotMode.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "outboxsmtit")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "outboxsmtit\\.outbox");
    }

    @Override
    protected String getSchemaNamePrefix() {
        return "test_server.outboxsmtit.outbox.";
    }

    @Override
    protected Schema getIdSchema() {
        return Uuid.builder().schema();
    }

    @Override
    protected Schema getPayloadSchema() {
        return Json.builder().optional().build();
    }

    @Override
    protected String tableName() {
        return "outboxsmtit.outbox";
    }

    @Override
    protected String topicName() {
        return TestHelper.topicName("outboxsmtit.outbox");
    }

    @Override
    protected void createTable() throws Exception {
        TestHelper.execute(SETUP_OUTBOX_SCHEMA);
        TestHelper.execute(SETUP_OUTBOX_TABLE);
    }

    @Override
    protected String createInsert(String eventId,
                                  String eventType,
                                  String aggregateType,
                                  String aggregateId,
                                  String payloadJson,
                                  String additional) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO outboxsmtit.outbox VALUES (");
        insert.append("'").append(UUID.fromString(eventId)).append("'");
        insert.append(", '").append(aggregateType).append("'");
        insert.append(", '").append(aggregateId).append("'");
        insert.append(", '").append(eventType).append("'");

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

    @Override
    protected void waitForSnapshotCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }

    @Override
    protected void alterTableWithExtra4Fields() throws Exception {
        TestHelper.execute("ALTER TABLE outboxsmtit.outbox add version int not null;");
        TestHelper.execute("ALTER TABLE outboxsmtit.outbox add somebooltype boolean not null;");
        TestHelper.execute("ALTER TABLE outboxsmtit.outbox add createdat timestamp without time zone not null;");
        TestHelper.execute("ALTER TABLE outboxsmtit.outbox add is_deleted boolean default false;");
    }

    @Override
    protected void alterTableWithTimestampField() throws Exception {
        TestHelper.execute("ALTER TABLE outboxsmtit.outbox add createdat timestamp without time zone not null;");
    }

    @Override
    protected void alterTableModifyPayload() throws Exception {
        TestHelper.execute("ALTER TABLE outboxsmtit.outbox ALTER COLUMN payload SET DATA TYPE VARCHAR(1000);");
    }

    @Override
    protected String getAdditionalFieldValues(boolean deleted) {
        if (deleted) {
            return ", 1, true, TIMESTAMP(3) '2019-03-24 20:52:59', true";
        }
        return ", 1, true, TIMESTAMP(3) '2019-03-24 20:52:59'";
    }

    @Override
    protected String getAdditionalFieldValuesTimestampOnly() {
        return ", TIMESTAMP '2019-03-24 20:52:59'";
    }
}
