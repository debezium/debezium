/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.nio.file.Path;
import java.util.UUID;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.converters.AbstractCloudEventsConverterTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

/**
 * Integration test for {@link io.debezium.converters.CloudEventsConverter} with binlog-based connectors.
 *
 * @author Roman Kudryashov
 */
public abstract class BinlogCloudEventsConverterIT<C extends SourceConnector>
        extends AbstractCloudEventsConverterTest<C>
        implements BinlogConnectorTest<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "empty")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private JdbcConnection connection;

    private static final String SETUP_TABLE = "CREATE TABLE a " +
            "(" +
            "  pk            integer      not null," +
            "  aa            integer      not null," +
            "  CONSTRAINT a_pk PRIMARY KEY (pk));";

    private static final String SETUP_OUTBOX_TABLE = "CREATE TABLE outbox " +
            "(" +
            "  id                   varchar(36)  not null," +
            "  aggregatetype        varchar(255) not null," +
            "  aggregateid          varchar(255) not null," +
            "  event_type           varchar(255) not null," +
            "  tracingspancontext   varchar(255)," +
            "  payload              json," +
            "  CONSTRAINT outbox_pk PRIMARY KEY (id));";

    private static final String INSERT_STMT = "INSERT INTO a VALUES (1, 1);";

    @Before
    @Override
    public void beforeEach() throws Exception {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            this.connection = db.connect();
        }
        super.beforeEach();
    }

    @After
    public void afterEach() throws Exception {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }

        dropAllDatabases();
    }

    @Override
    protected String getServerName() {
        return DATABASE.getServerName();
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return this.connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    @Override
    protected String topicName() {
        return DATABASE.topicForTable("a");
    }

    @Override
    protected String topicNameOutbox() {
        return DATABASE.topicForTable("outbox");
    }

    @Override
    protected void createTable() throws Exception {
        this.connection.execute(SETUP_TABLE);
    }

    @Override
    protected void createOutboxTable() throws Exception {
        this.connection.execute(SETUP_OUTBOX_TABLE);
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
        insert.append("INSERT INTO outbox VALUES (");
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
            insert.append(", null");
        }
        else if (payloadJson.isEmpty()) {
            insert.append(", ''");
        }
        else {
            insert.append(", '").append(payloadJson).append("'");
        }

        if (additional != null) {
            insert.append(additional);
        }
        insert.append(")");

        return insert.toString();
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
    }

    private TableId tableNameId() {
        return tableNameId("a");
    }

    private TableId tableNameId(String table) {
        return TableId.parse(DATABASE.qualifiedTableName(table));
    }
}
