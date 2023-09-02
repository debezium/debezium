/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.UUID;

import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.converters.AbstractCloudEventsConverterTest;
import io.debezium.jdbc.JdbcConnection;

/**
 * Integration test for {@link io.debezium.converters.CloudEventsConverter} with {@link PostgresConnector}
 *
 * @author Roman Kudryashov
 */
public class CloudEventsConverterIT extends AbstractCloudEventsConverterTest<PostgresConnector> {

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
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "outboxsmtit")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "outboxsmtit\\.outbox");
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
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }
}
