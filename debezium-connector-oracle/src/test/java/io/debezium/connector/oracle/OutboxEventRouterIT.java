/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.converters.NumberOneToBooleanConverter;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.transforms.outbox.AbstractEventRouterTest;
import io.debezium.transforms.outbox.EventRouter;
import io.debezium.util.Testing;

/**
 * An integration test for Oracle and the {@link EventRouter} for outbox.
 *
 * @author Chris Cranford
 */
public class OutboxEventRouterIT extends AbstractEventRouterTest<OracleConnector> {

    private static final String SETUP_OUTBOX_TABLE = "CREATE TABLE debezium.outbox (" +
            "id varchar2(64) not null primary key, " +
            "aggregatetype varchar2(255) not null, " +
            "aggregateid varchar2(255) not null, " +
            "type varchar2(255) not null, " +
            "payload varchar2(4000))";

    private OracleConnection connection;

    @Before
    @Override
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        super.beforeEach();
    }

    @After
    @Override
    public void afterEach() throws Exception {
        super.afterEach();
        if (connection != null && connection.isConnected()) {
            TestHelper.dropTable(connection, tableName());
            connection.close();
        }
    }

    @Override
    protected Class<OracleConnector> getConnectorClass() {
        return OracleConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder(boolean initialSnapshot) {
        final SnapshotMode snapshotMode = initialSnapshot ? SnapshotMode.INITIAL : SnapshotMode.NO_DATA;
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, snapshotMode.getValue())
                // this allows numeric(1) to be simulated as boolean types like other databases
                .with(OracleConnectorConfig.CUSTOM_CONVERTERS, "boolean")
                .with("boolean.type", NumberOneToBooleanConverter.class.getName())
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.OUTBOX");
    }

    @Override
    protected String getSchemaNamePrefix() {
        return "server1.DEBEZIUM.OUTBOX.";
    }

    @Override
    protected Schema getPayloadSchema() {
        return Schema.OPTIONAL_STRING_SCHEMA;
    }

    @Override
    protected String tableName() {
        return "debezium.outbox";
    }

    @Override
    protected String topicName() {
        return TestHelper.SERVER_NAME + ".DEBEZIUM.OUTBOX";
    }

    @Override
    protected void createTable() throws Exception {
        TestHelper.dropTable(connection, tableName());
        connection.execute(SETUP_OUTBOX_TABLE);
        TestHelper.streamTable(connection, tableName());
    }

    @Override
    protected String createInsert(String eventId,
                                  String eventType,
                                  String aggregateType,
                                  String aggregateId,
                                  String payloadJson,
                                  String additional) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO debezium.outbox VALUES (");
        insert.append("'").append(eventId).append("', ");
        insert.append("'").append(aggregateType).append("', ");
        insert.append("'").append(aggregateId).append("', ");
        insert.append("'").append(eventType).append("', ");
        if (payloadJson != null) {
            insert.append("'").append(payloadJson).append("'");
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

    @Override
    protected void waitForSnapshotCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    @Override
    protected void alterTableWithExtra4Fields() throws Exception {
        connection.execute("ALTER TABLE debezium.outbox add version numeric(9,0) not null");
        connection.execute("ALTER TABLE debezium.outbox add somebooltype numeric(1,0) not null");
        connection.execute("ALTER TABLE debezium.outbox add createdat timestamp not null");
        connection.execute("ALTER TABLE debezium.outbox add is_deleted numeric(1,0) default 0");
    }

    @Override
    protected void alterTableWithTimestampField() throws Exception {
        connection.execute("ALTER TABLE debezium.outbox add createdat timestamp not null");
    }

    @Override
    protected void alterTableModifyPayload() throws Exception {
        connection.execute("ALTER TABLE debezium.outbox modify (payload varchar2(1000))");
    }

    @Override
    protected String getAdditionalFieldValues(boolean deleted) {
        return ", 1, 1, TO_TIMESTAMP('2019-03-24 20:52:59', 'YYYY-MM-DD HH24:MI:SS'), " + (deleted ? "1" : "0");
    }

    @Override
    protected String getAdditionalFieldValuesTimestampOnly() {
        return ", TO_TIMESTAMP('2019-03-24 20:52:59', 'YYYY-MM-DD HH24:MI:SS')";
    }

    @Override
    protected String getFieldEventType() {
        return super.getFieldEventType().toUpperCase();
    }

    @Override
    protected String getFieldSchemaVersion() {
        return super.getFieldSchemaVersion().toUpperCase();
    }

    @Override
    protected String getFieldEventTimestamp() {
        return super.getFieldEventTimestamp().toUpperCase();
    }

    @Override
    protected String getFieldAggregateType() {
        return super.getFieldAggregateType().toUpperCase();
    }

    @Override
    protected String getSomeBoolType() {
        return super.getSomeBoolType().toUpperCase();
    }

    @Override
    protected String getIsDeleted() {
        return super.getIsDeleted().toUpperCase();
    }
}
