/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.connector.binlog.BinlogConnectorConfig.isBuiltInDatabase;
import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.EmbeddedEngine.CompletionResult;
import io.debezium.engine.DebeziumEngine;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.schema.DatabaseSchema;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public abstract class BinlogConnectorIT<C extends SourceConnector, P extends BinlogPartition, O extends BinlogOffsetContext<?>> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private final UniqueDatabase DATABASE_CUSTOM_SNAPSHOT = TestHelper.getUniqueDatabase("myServer1", "custom_snapshot")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);
    private final UniqueDatabase RO_DATABASE = TestHelper.getUniqueDatabase("myServer2", "connector_test_ro", DATABASE)
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    // Defines how many initial events are generated from loading the test databases.
    private static final int PRODUCTS_TABLE_EVENT_COUNT = 9;
    private static final int ORDERS_TABLE_EVENT_COUNT = 5;
    private static final int INITIAL_EVENT_COUNT = PRODUCTS_TABLE_EVENT_COUNT + 9 + 4 + ORDERS_TABLE_EVENT_COUNT + 6;

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        DATABASE_CUSTOM_SNAPSHOT.createAndInitialize();
        RO_DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    protected UniqueDatabase getDatabase() {
        return DATABASE;
    }

    /**
     * Verifies that the connector doesn't run with an invalid configuration. This does not actually connect to the MySQL server.
     */
    @Test
    public void shouldNotStartWithInvalidConfiguration() {
        config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(KafkaSchemaHistory.TOPIC, "myserver")
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages and exceptions will appear in the log");
        start(getConnectorClass(), config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldFailToValidateInvalidConfiguration() {
        final Configuration config = Configuration.create()
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();
        assertInvalidConfiguration(validateConfiguration(config));
    }

    @Test
    public void shouldValidateAcceptableConfiguration() {
        final Configuration config = Configuration.create().build();
        assertValidConfiguration(validateConfiguration(config));
    }

    protected abstract Config validateConfiguration(Configuration configuration);

    protected void assertInvalidConfiguration(Config result) {
        assertConfigurationErrors(result, BinlogConnectorConfig.HOSTNAME, 1);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.PORT);
        assertConfigurationErrors(result, BinlogConnectorConfig.USER, 1);
        assertConfigurationErrors(result, CommonConnectorConfig.TOPIC_PREFIX, 1);
        assertConfigurationErrors(result, BinlogConnectorConfig.SERVER_ID);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.TABLES_IGNORE_BUILTIN);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.DATABASE_INCLUDE_LIST);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.DATABASE_EXCLUDE_LIST);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.TABLE_INCLUDE_LIST);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.TABLE_EXCLUDE_LIST);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.COLUMN_EXCLUDE_LIST);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.COLUMN_INCLUDE_LIST);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.CONNECTION_TIMEOUT_MS);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.KEEP_ALIVE);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.KEEP_ALIVE_INTERVAL_MS);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.MAX_QUEUE_SIZE);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.MAX_BATCH_SIZE);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.POLL_INTERVAL_MS);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.SCHEMA_HISTORY);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.SNAPSHOT_MODE);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.SSL_KEYSTORE);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.SSL_KEYSTORE_PASSWORD);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.SSL_TRUSTSTORE);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.DECIMAL_HANDLING_MODE);
        assertNoConfigurationErrors(result, BinlogConnectorConfig.TIME_PRECISION_MODE);
    }

    protected void assertValidConfiguration(Config result) {
        // validate that the required fields have errors
        assertConfigurationErrors(result, BinlogConnectorConfig.HOSTNAME, 1);
        assertConfigurationErrors(result, BinlogConnectorConfig.USER, 1);
        assertConfigurationErrors(result, BinlogConnectorConfig.SERVER_ID, 1);
        assertConfigurationErrors(result, CommonConnectorConfig.TOPIC_PREFIX, 1);

        // validate the non required fields
        validateConfigField(result, BinlogConnectorConfig.PORT, 3306);
        validateConfigField(result, BinlogConnectorConfig.PASSWORD, null);
        validateConfigField(result, BinlogConnectorConfig.ON_CONNECT_STATEMENTS, null);
        validateConfigField(result, BinlogConnectorConfig.TABLES_IGNORE_BUILTIN, Boolean.TRUE);
        validateConfigField(result, BinlogConnectorConfig.DATABASE_INCLUDE_LIST, null);
        validateConfigField(result, BinlogConnectorConfig.DATABASE_EXCLUDE_LIST, null);
        validateConfigField(result, BinlogConnectorConfig.TABLE_INCLUDE_LIST, null);
        validateConfigField(result, BinlogConnectorConfig.TABLE_EXCLUDE_LIST, null);
        validateConfigField(result, BinlogConnectorConfig.COLUMN_EXCLUDE_LIST, null);
        validateConfigField(result, BinlogConnectorConfig.COLUMN_INCLUDE_LIST, null);
        validateConfigField(result, BinlogConnectorConfig.MSG_KEY_COLUMNS, null);
        validateConfigField(result, BinlogConnectorConfig.CONNECTION_TIMEOUT_MS, 30000);
        validateConfigField(result, BinlogConnectorConfig.KEEP_ALIVE, Boolean.TRUE);
        validateConfigField(result, BinlogConnectorConfig.KEEP_ALIVE_INTERVAL_MS, 60000L);
        validateConfigField(result, BinlogConnectorConfig.MAX_QUEUE_SIZE, 8192);
        validateConfigField(result, BinlogConnectorConfig.MAX_BATCH_SIZE, 2048);
        validateConfigField(result, BinlogConnectorConfig.POLL_INTERVAL_MS, 500L);
        validateConfigField(result, BinlogConnectorConfig.SCHEMA_HISTORY, "io.debezium.storage.kafka.history.KafkaSchemaHistory");
        validateConfigField(result, BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, Boolean.TRUE);
        validateConfigField(result, BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL);
        validateConfigField(result, BinlogConnectorConfig.SSL_KEYSTORE, null);
        validateConfigField(result, BinlogConnectorConfig.SSL_KEYSTORE_PASSWORD, null);
        validateConfigField(result, BinlogConnectorConfig.SSL_TRUSTSTORE, null);
        validateConfigField(result, BinlogConnectorConfig.SSL_TRUSTSTORE_PASSWORD, null);
        validateConfigField(result, BinlogConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE);
        validateConfigField(result, BinlogConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
    }

    protected <T> void validateConfigField(Config config, Field field, T expectedValue) {
        assertNoConfigurationErrors(config, field);
        Object actualValue = configValue(config, field.name()).value();
        if (actualValue == null) {
            actualValue = field.defaultValue();
        }
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        }
        else {
            if (expectedValue instanceof EnumeratedValue) {
                assertThat(((EnumeratedValue) expectedValue).getValue()).isEqualTo(actualValue.toString());
            }
            else {
                assertThat(expectedValue).isEqualTo(actualValue);
            }
        }
    }

    /**
     * Validates that SNAPSHOT_LOCKING_MODE 'none' is valid with all snapshot modes
     */
    @Test
    @FixFor("DBZ-639")
    public void shouldValidateLockingModeNoneWithValidSnapshotModeConfiguration() {
        final List<String> acceptableValues = Arrays.stream(SnapshotMode.values())
                .map(SnapshotMode::getValue)
                .collect(Collectors.toList());

        // Loop over all known valid values
        for (final String acceptableValue : acceptableValues) {
            Configuration config = DATABASE.defaultJdbcConfigBuilder()
                    .with(BinlogConnectorConfig.SERVER_ID, 18765)
                    .with(CommonConnectorConfig.TOPIC_PREFIX, "myServer")
                    .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "some.host.com")
                    .with(KafkaSchemaHistory.TOPIC, "my.db.history.topic")
                    .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    // Conflicting properties under test:
                    .with(getSnapshotLockingModeField(), getSnapshotLockingModeNone())
                    .with(BinlogConnectorConfig.SNAPSHOT_MODE, acceptableValue)
                    .build();

            Config result = validateConfiguration(config);
            assertNoConfigurationErrors(result, getSnapshotLockingModeField());
            assertSnapshotLockingModeIsNone(config);
        }
    }

    protected abstract Field getSnapshotLockingModeField();

    protected abstract String getSnapshotLockingModeNone();

    protected abstract void assertSnapshotLockingModeIsNone(Configuration config);

    private Optional<Header> getPKUpdateNewKeyHeader(SourceRecord record) {
        return this.getHeaderField(record, RelationalChangeRecordEmitter.PK_UPDATE_NEWKEY_FIELD);
    }

    private Optional<Header> getPKUpdateOldKeyHeader(SourceRecord record) {
        return this.getHeaderField(record, RelationalChangeRecordEmitter.PK_UPDATE_OLDKEY_FIELD);
    }

    private Optional<Header> getHeaderField(SourceRecord record, String fieldName) {
        return StreamSupport.stream(record.headers().spliterator(), false)
                .filter(header -> fieldName.equals(header.key()))
                .findFirst();
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingSnapshot() throws SQLException, InterruptedException {
        shouldConsumeAllEventsFromDatabaseUsingSnapshotByField(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, 18765);
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingSnapshotOld() throws SQLException, InterruptedException {
        shouldConsumeAllEventsFromDatabaseUsingSnapshotByField(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, 18775);
    }

    private void shouldConsumeAllEventsFromDatabaseUsingSnapshotByField(Field dbIncludeListField, int serverId) throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = DATABASE.defaultJdbcConfigBuilder()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.SERVER_ID, serverId)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(dbIncludeListField, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(5 + 9 + 9 + 4 + 11 + 1); // 11 schema change records + 1 SET statement
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(12);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products")).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products_on_hand")).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("customers")).size()).isEqualTo(4);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("orders")).size()).isEqualTo(5);
        assertThat(records.topics().size()).isEqualTo(5);
        assertThat(records.databaseNames().size()).isEqualTo(2);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(11);
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1);
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // Check that the last record has snapshots disabled in the offset, but not in the source
        List<SourceRecord> allRecords = records.allRecordsInOrder();
        SourceRecord last = allRecords.get(allRecords.size() - 1);
        SourceRecord secondToLast = allRecords.get(allRecords.size() - 2);
        assertThat(secondToLast.sourceOffset().containsKey(BinlogSourceInfo.SNAPSHOT_KEY)).isTrue();
        assertThat(last.sourceOffset().containsKey(BinlogSourceInfo.SNAPSHOT_KEY)).isFalse(); // not snapshot
        assertThat(((Struct) secondToLast.value()).getStruct(Envelope.FieldName.SOURCE).getString(BinlogSourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
        assertThat(((Struct) last.value()).getStruct(Envelope.FieldName.SOURCE).getString(BinlogSourceInfo.SNAPSHOT_KEY)).isEqualTo("last");

        // ---------------------------------------------------------------------------------------------------------------
        // Stopping the connector does not lose events recorded when connector is not running
        // ---------------------------------------------------------------------------------------------------------------

        // Make sure there are no more events and then stop the connector ...
        waitForAvailableRecords(3, TimeUnit.SECONDS);
        int totalConsumed = consumeAvailableRecords(this::print);
        System.out.println("TOTAL CONSUMED = " + totalConsumed);
        // assertThat(totalConsumed).isEqualTo(0);
        stopConnector();

        // Make some changes to data only while the connector is stopped ...
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.query("SELECT * FROM products", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
                connection.execute("INSERT INTO products VALUES (default,'robot','Toy robot',1.304);");
                connection.query("SELECT * FROM products", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
            }
        }

        // Testing.Print.enable();

        // Restart the connector and read the insert record ...
        Testing.print("*** Restarting connector after inserts were made");
        start(getConnectorClass(), config);
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1);
        List<SourceRecord> inserts = records.recordsForTopic(DATABASE.topicForTable("products"));
        assertInsert(inserts.get(0), "id", 110);
        Testing.print("*** Done with inserts and restart");

        Testing.print("*** Stopping connector");
        stopConnector();
        Testing.print("*** Restarting connector");
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Simple INSERT
        // ---------------------------------------------------------------------------------------------------------------
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO products VALUES (1001,'roy','old robot',1234.56);");
                connection.query("SELECT * FROM products", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
            }
        }

        // Testing.Print.enable();
        // And consume the one insert ...
        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1);
        inserts = records.recordsForTopic(DATABASE.topicForTable("products"));
        assertInsert(inserts.get(0), "id", 1001);

        Testing.print("*** Done with simple insert");

        // ---------------------------------------------------------------------------------------------------------------
        // Changing the primary key of a row should result in 3 events: INSERT, DELETE, and TOMBSTONE
        // ---------------------------------------------------------------------------------------------------------------
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE products SET id=2001, description='really old robot' WHERE id=1001");
                connection.query("SELECT * FROM products", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
            }
        }
        // And consume the update of the PK, which is one insert followed by a delete followed by a tombstone ...
        records = consumeRecordsByTopic(3);
        List<SourceRecord> updates = records.recordsForTopic(DATABASE.topicForTable("products"));
        assertThat(updates.size()).isEqualTo(3);

        SourceRecord deleteRecord = updates.get(0);
        assertDelete(deleteRecord, "id", 1001);

        Header keyPKUpdateHeader = getPKUpdateNewKeyHeader(deleteRecord).get();
        assertEquals(Integer.valueOf(2001), ((Struct) keyPKUpdateHeader.value()).getInt32("id"));

        assertTombstone(updates.get(1), "id", 1001);

        SourceRecord insertRecord = updates.get(2);
        assertInsert(insertRecord, "id", 2001);

        keyPKUpdateHeader = getPKUpdateOldKeyHeader(insertRecord).get();
        assertEquals(Integer.valueOf(1001), ((Struct) keyPKUpdateHeader.value()).getInt32("id"));

        Testing.print("*** Done with PK change");

        // ---------------------------------------------------------------------------------------------------------------
        // Simple UPDATE (with no schema changes)
        // ---------------------------------------------------------------------------------------------------------------
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE products SET weight=1345.67 WHERE id=2001");
                connection.query("SELECT * FROM products", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
            }
        }

        // And consume the one update ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        updates = records.recordsForTopic(DATABASE.topicForTable("products"));
        assertThat(updates.size()).isEqualTo(1);
        assertUpdate(updates.get(0), "id", 2001);
        updates.forEach(this::validate);

        Testing.print("*** Done with simple update");

        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Change our schema with a fully-qualified name; we should still see this event
        // ---------------------------------------------------------------------------------------------------------------
        // Add a column with default to the 'products' table and explicitly update one record ...
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(String.format(
                        "ALTER TABLE %s.products ADD COLUMN volume FLOAT, ADD COLUMN alias VARCHAR(30) NULL AFTER description",
                        DATABASE.getDatabaseName()));
                connection.execute("UPDATE products SET volume=13.5 WHERE id=2001");
                connection.query("SELECT * FROM products", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
            }
        }

        // And consume the one schema change event and one update event ...
        records = consumeRecordsByTopic(2);
        assertThat(records.topics().size()).isEqualTo(2);
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(1);
        updates = records.recordsForTopic(DATABASE.topicForTable("products"));
        assertThat(updates.size()).isEqualTo(1);
        assertUpdate(updates.get(0), "id", 2001);
        updates.forEach(this::validate);

        Testing.print("*** Done with schema change (same db and fully-qualified name)");

        // ---------------------------------------------------------------------------------------------------------------
        // DBZ-55 Change our schema using a different database and a fully-qualified name; we should still see this event
        // ---------------------------------------------------------------------------------------------------------------
        // Connect to a different database, but use the fully qualified name for a table in our database ...
        try (BinlogTestConnection db = getTestDatabaseConnection("emptydb");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(String.format("CREATE TABLE %s.stores ("
                        + " id INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,"
                        + " first_name VARCHAR(255) NOT NULL,"
                        + " last_name VARCHAR(255) NOT NULL,"
                        + " email VARCHAR(255) NOT NULL );", DATABASE.getDatabaseName()));
            }
        }

        // And consume the one schema change event only ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(1);
        records.recordsForTopic(DATABASE.getServerName()).forEach(this::validate);

        Testing.print("*** Done with PK change (different db and fully-qualified name)");

        // ---------------------------------------------------------------------------------------------------------------
        // Make sure there are no additional events
        // ---------------------------------------------------------------------------------------------------------------

        // Do something completely different with a table we've not modified yet and then read that event.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE products_on_hand SET quantity=20 WHERE product_id=109");
                connection.query("SELECT * FROM products_on_hand", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
            }
        }

        // And make sure we consume that one update ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        updates = records.recordsForTopic(DATABASE.topicForTable("products_on_hand"));
        assertThat(updates.size()).isEqualTo(1);
        assertUpdate(updates.get(0), "product_id", 109);
        updates.forEach(this::validate);

        Testing.print("*** Done with verifying no additional events");

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector ...
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();

        // ---------------------------------------------------------------------------------------------------------------
        // Restart the connector to read only part of a transaction ...
        // ---------------------------------------------------------------------------------------------------------------
        Testing.print("*** Restarting connector");
        CompletionResult completion = new CompletionResult();
        start(getConnectorClass(), config, completion, (record) -> {
            // We want to stop before processing record 3003 ...
            Struct key = (Struct) record.key();
            Number id = (Number) key.get("id");
            if (id.intValue() == 3003) {
                return true;
            }
            return false;
        });

        BinlogPosition positionBeforeInserts = new BinlogPosition();
        BinlogPosition positionAfterInserts = new BinlogPosition();
        BinlogPosition positionAfterUpdate = new BinlogPosition();
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                var statusStmt = db.binaryLogStatusStatement();
                connection.query(statusStmt, positionBeforeInserts::readFromDatabase);
                connection.execute("INSERT INTO products(id,name,description,weight,volume,alias) VALUES "
                        + "(3001,'ashley','super robot',34.56,0.00,'ashbot'), "
                        + "(3002,'arthur','motorcycle',87.65,0.00,'arcycle'), "
                        + "(3003,'oak','tree',987.65,0.00,'oak');");
                connection.query("SELECT * FROM products", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
                connection.query(statusStmt, positionAfterInserts::readFromDatabase);
                // Change something else that is unrelated ...
                connection.execute("UPDATE products_on_hand SET quantity=40 WHERE product_id=109");
                connection.query("SELECT * FROM products_on_hand", rs -> {
                    if (Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
                connection.query(statusStmt, positionAfterUpdate::readFromDatabase);
            }
        }

        // Testing.Print.enable();

        // And consume the one insert ...
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products")).size()).isEqualTo(2);
        assertThat(records.topics().size()).isEqualTo(1);
        inserts = records.recordsForTopic(DATABASE.topicForTable("products"));
        assertInsert(inserts.get(0), "id", 3001);
        assertInsert(inserts.get(1), "id", 3002);

        // Verify that the connector has stopped ...
        completion.await(10, TimeUnit.SECONDS);
        assertThat(completion.hasCompleted()).isTrue();
        assertThat(completion.hasError()).isTrue();
        assertThat(completion.success()).isFalse();
        assertNoRecordsToConsume();
        assertConnectorNotRunning();

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector ...
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();

        // Read the last committed offsets, and verify the binlog coordinates ...
        final String serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
        final Map<String, String> partition = createPartition(serverName, DATABASE.getDatabaseName()).getSourcePartition();
        Map<String, ?> lastCommittedOffset = readLastCommittedOffset(config, partition);
        final O offsetContext = loadOffsets(Configuration.create().with(CommonConnectorConfig.TOPIC_PREFIX, serverName).build(), lastCommittedOffset);
        final BinlogSourceInfo persistedOffsetSource = offsetContext.getSource();
        Testing.print("Position before inserts: " + positionBeforeInserts);
        Testing.print("Position after inserts:  " + positionAfterInserts);
        Testing.print("Offset: " + lastCommittedOffset);
        Testing.print("Position after update:  " + positionAfterUpdate);
        if (replicaIsMaster) {
            // Same binlog filename ...
            assertThat(persistedOffsetSource.binlogFilename()).isEqualTo(positionBeforeInserts.binlogFilename());
            assertThat(persistedOffsetSource.binlogFilename()).isEqualTo(positionAfterInserts.binlogFilename());
            assertBinlogPosition(persistedOffsetSource.binlogPosition(), positionBeforeInserts.binlogPosition);
            assertThat(persistedOffsetSource.binlogPosition()).isLessThan(positionAfterInserts.binlogPosition());
        }
        else {
            // the replica is not the same server as the master, so it will have a different binlog filename and position ...
        }
        // Event number is 2 ...
        assertThat(offsetContext.eventsToSkipUponRestart()).isEqualTo(2);
        // GTID set should match the before-inserts GTID set ...
        // assertThat(persistedOffsetSource.gtidSet()).isEqualTo(positionBeforeInserts.gtidSet());

        Testing.print("*** Restarting connector, and should begin with inserting 3003 (not 109!)");
        start(getConnectorClass(), config);

        // And consume the insert for 3003 ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        inserts = records.recordsForTopic(DATABASE.topicForTable("products"));
        if (inserts == null) {
            updates = records.recordsForTopic(DATABASE.topicForTable("products_on_hand"));
            if (updates != null) {
                fail("Restarted connector and missed the insert of product id=3003!");
            }
        }
        // Read the first record produced since we've restarted
        SourceRecord prod3003 = inserts.get(0);
        assertInsert(prod3003, "id", 3003);

        // Check that the offset has the correct/expected values ...
        assertOffset(prod3003, "file", lastCommittedOffset.get("file"));
        assertOffset(prod3003, "pos", lastCommittedOffset.get("pos"));
        assertOffset(prod3003, "row", 3);
        assertOffset(prod3003, "event", lastCommittedOffset.get("event"));

        // Check that the record has all of the column values ...
        assertValueField(prod3003, "after/id", 3003);
        assertValueField(prod3003, "after/name", "oak");
        assertValueField(prod3003, "after/description", "tree");
        assertValueField(prod3003, "after/weight", 987.65d);
        assertValueField(prod3003, "after/volume", 0.0d);
        assertValueField(prod3003, "after/alias", "oak");

        // And make sure we consume that one extra update ...
        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        updates = records.recordsForTopic(DATABASE.topicForTable("products_on_hand"));
        assertThat(updates.size()).isEqualTo(1);
        assertUpdate(updates.get(0), "product_id", 109);
        updates.forEach(this::validate);

        // Start the connector again, and we should see the next two
        Testing.print("*** Done with simple insert");
    }

    protected abstract P createPartition(String serverName, String databaseName);

    protected abstract O loadOffsets(Configuration configuration, Map<String, ?> offsets);

    protected abstract void assertBinlogPosition(long offsetPosition, long beforeInsertsPosition);

    @Test
    public void shouldUseOverriddenSelectStatementDuringSnapshotting() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        config = DATABASE.defaultJdbcConfigBuilder()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.SERVER_ID, 28765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.getDatabaseName() + ".products")
                .with(BinlogConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, DATABASE.getDatabaseName() + ".products")
                .with(BinlogConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + DATABASE.getDatabaseName() + ".products",
                        String.format("SELECT * from %s.products where id>=108 order by id", DATABASE.getDatabaseName()))
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(6 + 2); // 6 DDL and 2 insert records
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(6);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products")).size()).isEqualTo(2);

        // check that only the expected records are retrieved, in-order
        assertThat(((Struct) records.recordsForTopic(DATABASE.topicForTable("products")).get(0).key()).getInt32("id")).isEqualTo(108);
        assertThat(((Struct) records.recordsForTopic(DATABASE.topicForTable("products")).get(1).key()).getInt32("id")).isEqualTo(109);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
    }

    @Test
    public void shouldUseMultipleOverriddenSelectStatementsDuringSnapshotting() throws SQLException, InterruptedException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        String tables = String.format("%s.products,%s.products_on_hand", DATABASE.getDatabaseName(), DATABASE.getDatabaseName());
        config = DATABASE.defaultJdbcConfigBuilder()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.SERVER_ID, 28765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, tables)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(BinlogConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, tables)
                .with(BinlogConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + DATABASE.getDatabaseName() + ".products",
                        String.format("SELECT * from %s.products where id>=108 order by id", DATABASE.getDatabaseName()))
                .with(BinlogConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + DATABASE.getDatabaseName() + ".products_on_hand",
                        String.format("SELECT * from %s.products_on_hand where product_id>=108 order by product_id", DATABASE.getDatabaseName()))
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Testing.Print.enable();

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(8 + 4); // 8 DDL and 4 insert records
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(8);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products")).size()).isEqualTo(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products_on_hand")).size()).isEqualTo(2);

        // check that only the expected records are retrieved, in-order
        assertThat(((Struct) records.recordsForTopic(DATABASE.topicForTable("products")).get(0).key()).getInt32("id")).isEqualTo(108);
        assertThat(((Struct) records.recordsForTopic(DATABASE.topicForTable("products")).get(1).key()).getInt32("id")).isEqualTo(109);
        assertThat(((Struct) records.recordsForTopic(DATABASE.topicForTable("products_on_hand")).get(0).key()).getInt32("product_id")).isEqualTo(108);
        assertThat(((Struct) records.recordsForTopic(DATABASE.topicForTable("products_on_hand")).get(1).key()).getInt32("product_id")).isEqualTo(109);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
    }

    @Test
    @FixFor("DBZ-977")
    public void shouldIgnoreAlterTableForNonCapturedTablesNotStoredInHistory() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        final String tables = String.format("%s.customers", DATABASE.getDatabaseName(), DATABASE.getDatabaseName());
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, tables)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(1 + 5);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(5);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("ALTER TABLE orders ADD COLUMN (newcol INT)");
                connection.execute("ALTER TABLE customers ADD COLUMN (newcol INT)");
                connection.execute("INSERT INTO customers VALUES "
                        + "(default,'name','surname','email',1);");
            }
        }

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("customers")).size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(1);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1201")
    public void shouldSaveSetCharacterSetWhenStoringOnlyCapturededTables() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        config = DATABASE.defaultConfig()
                .with(CommonConnectorConfig.FAIL_ON_NO_TABLES, false)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, "no_" + DATABASE.getDatabaseName())
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1246")
    public void shouldProcessCreateUniqueIndex() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        final String tables = String.format("%s.migration_test", DATABASE.getDatabaseName(), DATABASE.getDatabaseName());
        config = DATABASE.defaultConfig()
                .with(CommonConnectorConfig.FAIL_ON_NO_TABLES, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, tables)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Wait for streaming to start
        // During the snapshot phase, 11 events in total should be generated
        // 4 drop tables, 4 create tables
        // 1 drop database, 1 create database, and 1 use database
        //
        // Prior to this being added, it was possible that the following code would create the migration_test
        // table prior to the snapshot actually starting, which would mean that a drop and create table event
        // would be emitted plus the create index, yielding 14 events total. But if snapshot started quicker
        // then we could observe 13 events emitted since the migration_table creation and its index would
        // only be seen by streaming phase.
        //
        // For consistency, this wait gurantees 11 events during snapshot and 2 during streaming.
        waitForStreamingRunning(DATABASE.getServerName());

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        "create table migration_test (id varchar(20) null,mgb_no varchar(20) null)",
                        "create unique index migration_test_mgb_no_uindex on migration_test (mgb_no)",
                        "insert into migration_test values(1,'2')");
            }
        }

        SourceRecords records = consumeRecordsByTopic(15);
        List<SourceRecord> migrationTestRecords = records.recordsForTopic(DATABASE.topicForTable("migration_test"));
        assertThat(migrationTestRecords.size()).isEqualTo(1);
        SourceRecord record = migrationTestRecords.get(0);
        assertThat(record.key()).isNull();
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(13);

        // Set column mgb_no to required, will treat this unique index column as primary key
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        "alter table migration_test change column mgb_no mgb_no varchar(20) not null",
                        "alter table migration_test drop index migration_test_mgb_no_uindex",
                        "create unique index migration_test_mgb_no_uindex on migration_test (mgb_no)",
                        "insert into migration_test values(2,'3')");
            }
        }

        records = consumeRecordsByTopic(4);
        migrationTestRecords = records.recordsForTopic(DATABASE.topicForTable("migration_test"));
        assertThat(migrationTestRecords.size()).isEqualTo(1);
        record = migrationTestRecords.get(0);
        assertThat(((Struct) record.key()).getString("mgb_no")).isEqualTo("3");

        stopConnector();
    }

    @Test
    @FixFor("DBZ-977")
    public void shouldIgnoreAlterTableForNonCapturedTablesStoredInHistory() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        final String tables = String.format("%s.customers", DATABASE.getDatabaseName(), DATABASE.getDatabaseName());
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, tables)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        dropDatabases();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 2 + 2 * 4);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(1 + 2 + 2 * 4);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("ALTER TABLE orders ADD COLUMN (newcol INT)");
                connection.execute("ALTER TABLE customers ADD COLUMN (newcol INT)");
                connection.execute("INSERT INTO customers VALUES "
                        + "(default,'name','surname','email',1);");
            }
        }

        records = consumeRecordsByTopic(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("customers")).size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(2);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1264")
    public void shouldIgnoreCreateIndexForNonCapturedTablesNotStoredInHistory() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        final String tables = String.format("%s.customers", DATABASE.getDatabaseName(), DATABASE.getDatabaseName());
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, tables)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        "CREATE TABLE nonmon (id INT)");
            }
        }
        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(6);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(5);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        "CREATE UNIQUE INDEX pk ON nonmon(id)",
                        "INSERT INTO customers VALUES (default,'name','surname','email');");
            }
        }

        final SourceRecord record = consumeRecord();
        assertThat(record.topic()).isEqualTo(DATABASE.topicForTable("customers"));
    }

    @Test
    @FixFor("DBZ-683")
    public void shouldReceiveSchemaForNonWhitelistedTablesAndDatabases() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        final String tables = String.format("%s.customers,%s.orders", DATABASE.getDatabaseName(), DATABASE.getDatabaseName());
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, tables)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, ".*")
                .build();

        dropDatabases();

        try (BinlogTestConnection db = getTestDatabaseConnection("mysql");) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        "CREATE DATABASE non_wh",
                        "USE non_wh",
                        "CREATE TABLE t1 (ID INT PRIMARY KEY)");
            }
        }

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        // Two databases
        // SET + USE + DROP DB + CREATE DB + 4 tables (2 whitelisted) (DROP + CREATE) TABLE
        // USE + DROP DB + CREATE DB + (DROP + CREATE) TABLE
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 2 + 2 * 4 + 1 + 2 + 2);
        // Records for one of the databases only
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(1 + 2 + 2 * 4);
        stopConnector();
    }

    @Test
    @FixFor("DBZ-1546")
    public void shouldHandleIncludeListTables() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        final String tables = String.format("%s.customers, %s.orders", DATABASE.getDatabaseName(), DATABASE.getDatabaseName());
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, tables)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, ".*")
                .build();

        dropDatabases();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        // Two databases
        // SET + USE + DROP DB + CREATE DB + 4 tables (2 whitelisted) (DROP + CREATE) TABLE
        // USE + DROP DB + CREATE DB + (DROP + CREATE) TABLE
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 2 + 2 * 4 + 1 + 2 + 2);
        // Records for one of the databases only
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(1 + 2 + 2 * 4);
        stopConnector();
    }

    @Test
    public void shouldHandleIncludedTables() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        final String tables = String.format("%s.customers, %s.orders", DATABASE.getDatabaseName(), DATABASE.getDatabaseName());
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, tables)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, ".*")
                .build();

        dropDatabases();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        // Two databases
        // SET + USE + DROP DB + CREATE DB + 4 tables (2 whitelisted) (DROP + CREATE) TABLE
        // USE + DROP DB + CREATE DB + (DROP + CREATE) TABLE
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 2 + 2 * 4 + 1 + 2 + 2);
        // Records for one of the databases only
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(1 + 2 + 2 * 4);
        stopConnector();
    }

    private void dropDatabases() throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection("mysql");) {
            try (JdbcConnection connection = db.connect()) {
                connection.query("SHOW DATABASES", rs -> {
                    while (rs.next()) {
                        final String dbName = rs.getString(1);
                        if (!isBuiltInDatabase(dbName) && !dbName.equals(DATABASE.getDatabaseName())) {
                            connection.execute("DROP DATABASE IF EXISTS `" + dbName + "`");
                        }
                    }
                });
            }
        }
    }

    protected static class BinlogPosition {
        private String binlogFilename;
        private long binlogPosition;
        private String gtidSet;

        public void readFromDatabase(ResultSet rs) throws SQLException {
            if (rs.next()) {
                binlogFilename = rs.getString(1);
                binlogPosition = rs.getLong(2);
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                }
            }
        }

        public String binlogFilename() {
            return binlogFilename;
        }

        public long binlogPosition() {
            return binlogPosition;
        }

        public String gtidSet() {
            return gtidSet;
        }

        public boolean hasGtids() {
            return gtidSet != null;
        }

        @Override
        public String toString() {
            return "file=" + binlogFilename + ", pos=" + binlogPosition + ", gtids=" + (gtidSet != null ? gtidSet : "");
        }
    }

    private Struct getAfter(SourceRecord record) {
        return (Struct) ((Struct) record.value()).get("after");
    }

    @Test
    public void shouldConsumeEventsWithNoSnapshot() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = RO_DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT); // 6 DDL changes
        assertThat(recordsForTopicForRoProductsTable(records).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("products_on_hand")).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("customers")).size()).isEqualTo(4);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("orders")).size()).isEqualTo(5);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("Products")).size()).isEqualTo(9);
        assertThat(records.topics().size()).isEqualTo(4 + 1);
        assertThat(records.ddlRecordsForDatabase(RO_DATABASE.getDatabaseName()).size()).isEqualTo(6);

        // check float value
        Optional<SourceRecord> recordWithScientfic = records.recordsForTopic(RO_DATABASE.topicForTable("Products")).stream()
                .filter(x -> "hammer2".equals(getAfter(x).get("name"))).findFirst();
        assertThat(recordWithScientfic.isPresent());
        assertThat(getAfter(recordWithScientfic.get()).get("weight")).isEqualTo(0.875f);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        records.recordsForTopic(RO_DATABASE.topicForTable("orders")).forEach(record -> {
            print(record);
        });

        records.recordsForTopic(RO_DATABASE.topicForTable("customers")).forEach(record -> {
            print(record);
        });
    }

    @Test
    @FixFor("DBZ-7570 - workaround")
    public void shouldConsumeEventsWithNonGracefulDisconnect() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = RO_DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.USE_NONGRACEFUL_DISCONNECT, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT); // 6 DDL changes
        assertThat(recordsForTopicForRoProductsTable(records).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("products_on_hand")).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("customers")).size()).isEqualTo(4);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("orders")).size()).isEqualTo(5);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("Products")).size()).isEqualTo(9);
        assertThat(records.topics().size()).isEqualTo(4 + 1);
        assertThat(records.ddlRecordsForDatabase(RO_DATABASE.getDatabaseName()).size()).isEqualTo(6);

        // check float value
        Optional<SourceRecord> recordWithScientfic = records.recordsForTopic(RO_DATABASE.topicForTable("Products")).stream()
                .filter(x -> "hammer2".equals(getAfter(x).get("name"))).findFirst();
        assertThat(recordWithScientfic.isPresent());
        assertThat(getAfter(recordWithScientfic.get()).get("weight")).isEqualTo(0.875f);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        records.recordsForTopic(RO_DATABASE.topicForTable("orders")).forEach(record -> {
            print(record);
        });

        records.recordsForTopic(RO_DATABASE.topicForTable("customers")).forEach(record -> {
            print(record);
        });
    }

    @Test
    @FixFor("DBZ-1962")
    public void shouldConsumeEventsWithIncludedColumns() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = RO_DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.COLUMN_INCLUDE_LIST, RO_DATABASE.qualifiedTableName("orders") + ".order_number")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(9 + 9 + 4 + 5 + 1);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("orders")).size()).isEqualTo(5);
        assertThat(records.topics().size()).isEqualTo(5);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        // Check that orders.order_number is present ...
        records.recordsForTopic(RO_DATABASE.topicForTable("orders")).forEach(record -> {
            print(record);
            Struct value = ((Struct) record.value()).getStruct("after");
            try {
                value.get("order_number");
            }
            catch (DataException e) {
                fail("The 'order_number' field was not found but should exist");
            }

            try {
                value.get("order_date");
                fail("The 'order_date' field was found but should be filtered");
            }
            catch (DataException e) {
                // Expected, this field should be filtered by the include list
            }
        });
    }

    @Test
    @FixFor("DBZ-2525")
    public void shouldConsumeEventsWithIncludedColumnsForKeywordNamedTable() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        try (BinlogTestConnection db = getTestDatabaseConnection(RO_DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(String.format("CREATE TABLE %s.`order` ("
                        + " id INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,"
                        + " `select` VARCHAR(255) NOT NULL,"
                        + " not_included VARCHAR(255) NOT NULL);", RO_DATABASE.getDatabaseName()));

                connection.execute(String.format("INSERT INTO %s.`order` VALUES (100001,'included','not included');", RO_DATABASE.getDatabaseName()));
            }
        }

        // Use the DB configuration to define the connector's configuration ...
        config = RO_DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, RO_DATABASE.qualifiedTableName("order"))
                .with(BinlogConnectorConfig.COLUMN_INCLUDE_LIST, RO_DATABASE.qualifiedTableName("order") + ".select")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("order")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        // Check that orders.order_number is present ...
        records.recordsForTopic(RO_DATABASE.topicForTable("order")).forEach(record -> {
            print(record);
            Struct value = ((Struct) record.value()).getStruct("after");
            try {
                value.get("select");
            }
            catch (DataException e) {
                fail("The 'select' field was not found but should exist");
            }

            try {
                value.get("not_included");
                fail("The 'not_included' field was found but should be filtered");
            }
            catch (DataException e) {
                // Expected, this field should be filtered by the include list
            }
        });
    }

    @Test
    public void shouldConsumeEventsWithMaskedAndBlacklistedColumns() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = RO_DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.COLUMN_EXCLUDE_LIST, RO_DATABASE.qualifiedTableName("orders") + ".order_number")
                .with("column.mask.with.12.chars", RO_DATABASE.qualifiedTableName("customers") + ".email")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(9 + 9 + 4 + 5 + 1);
        assertThat(recordsForTopicForRoProductsTable(records).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("products_on_hand")).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("customers")).size()).isEqualTo(4);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("orders")).size()).isEqualTo(5);
        assertThat(records.topics().size()).isEqualTo(5);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        // Check that the orders.order_number is not present ...
        records.recordsForTopic(RO_DATABASE.topicForTable("orders")).forEach(record -> {
            print(record);
            Struct value = (Struct) record.value();
            try {
                value.getStruct("after").get("order_number");
                fail("The 'order_number' field was found but should not exist");
            }
            catch (DataException e) {
                // expected
            }
        });

        // Check that the customer.email is masked ...
        records.recordsForTopic(RO_DATABASE.topicForTable("customers")).forEach(record -> {
            Struct value = (Struct) record.value();
            if (value.getStruct("after") != null) {
                assertThat(value.getStruct("after").getString("email")).isEqualTo("************");
            }
            if (value.getStruct("before") != null) {
                assertThat(value.getStruct("before").getString("email")).isEqualTo("************");
            }
            print(record);
        });
    }

    @Test
    @FixFor("DBZ-1692")
    public void shouldConsumeEventsWithMaskedHashedColumns() throws InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = RO_DATABASE.defaultConfig()
                .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K", RO_DATABASE.qualifiedTableName("customers") + ".email")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(9 + 9 + 4 + 5 + 1);
        assertThat(recordsForTopicForRoProductsTable(records)).hasSize(9);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("products_on_hand"))).hasSize(9);
        final List<SourceRecord> customers = records.recordsForTopic(RO_DATABASE.topicForTable("customers"));
        assertThat(customers).hasSize(4);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("orders"))).hasSize(5);
        assertThat(records.topics()).hasSize(5);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        // Check that the customer.email is masked ...
        Struct value1001 = (Struct) customers.get(0).value();
        if (value1001.getStruct("after") != null) {
            assertThat(value1001.getStruct("after").getString("email")).isEqualTo("d540e71abf15be8b51c7967397ba359db27d6f6ae85a297fe8d0d7005ffd0e82");
        }

        Struct value1002 = (Struct) customers.get(1).value();
        if (value1002.getStruct("after") != null) {
            assertThat(value1002.getStruct("after").getString("email")).isEqualTo("b1f1a1a63559c1d3a98bd7bb5c363d7e21a37463a7266bc2ff341eaef7ac8ef3");
        }

        Struct value1003 = (Struct) customers.get(2).value();
        if (value1003.getStruct("after") != null) {
            assertThat(value1003.getStruct("after").getString("email")).isEqualTo("bbe1de7b1068bc8f86bbb19f432ce1d44fbd461339916f42544b3f7ebff674d6");
        }

        Struct value1004 = (Struct) customers.get(3).value();
        if (value1004.getStruct("after") != null) {
            assertThat(value1004.getStruct("after").getString("email")).isEqualTo("ff21be44fb224e57d822ea9a51d343d77e4c49ac3dedd3d144024ac2012af0a1");
        }
    }

    @Test
    @FixFor("DBZ-1972")
    public void shouldConsumeEventsWithTruncatedColumns() throws InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = RO_DATABASE.defaultConfig()
                .with("column.truncate.to.7.chars", RO_DATABASE.qualifiedTableName("customers") + ".email")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(9 + 9 + 4 + 5 + 1);
        assertThat(recordsForTopicForRoProductsTable(records)).hasSize(9);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("products_on_hand"))).hasSize(9);
        final List<SourceRecord> customers = records.recordsForTopic(RO_DATABASE.topicForTable("customers"));
        assertThat(customers).hasSize(4);
        assertThat(records.recordsForTopic(RO_DATABASE.topicForTable("orders"))).hasSize(5);
        assertThat(records.topics()).hasSize(5);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        // Check that the customer.email is masked ...
        Struct value1001 = (Struct) customers.get(0).value();
        if (value1001.getStruct("after") != null) {
            assertThat(value1001.getStruct("after").getString("email")).isEqualTo("sally.t");
        }

        Struct value1002 = (Struct) customers.get(1).value();
        if (value1002.getStruct("after") != null) {
            assertThat(value1002.getStruct("after").getString("email")).isEqualTo("gbailey");
        }

        Struct value1003 = (Struct) customers.get(2).value();
        if (value1003.getStruct("after") != null) {
            assertThat(value1003.getStruct("after").getString("email")).isEqualTo("ed@walk");
        }

        Struct value1004 = (Struct) customers.get(3).value();
        if (value1004.getStruct("after") != null) {
            assertThat(value1004.getStruct("after").getString("email")).isEqualTo("annek@n");
        }
    }

    @Test
    @FixFor("DBZ-582")
    public void shouldEmitTombstoneOnDeleteByDefault() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT); // 6 DDL changes
        assertThat(records.recordsForTopic(DATABASE.topicForTable("orders")).size()).isEqualTo(5);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE orders SET order_number=10101 WHERE order_number=10001");
            }
        }
        // Consume the update of the PK, which is one insert followed by a delete followed by a tombstone ...
        records = consumeRecordsByTopic(3);
        List<SourceRecord> updates = records.recordsForTopic(DATABASE.topicForTable("orders"));
        assertThat(updates.size()).isEqualTo(3);
        assertDelete(updates.get(0), "order_number", 10001);
        assertTombstone(updates.get(1), "order_number", 10001);
        assertInsert(updates.get(2), "order_number", 10101);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("DELETE FROM orders WHERE order_number=10101");
            }
        }
        records = consumeRecordsByTopic(2);
        updates = records.recordsForTopic(DATABASE.topicForTable("orders"));
        assertThat(updates.size()).isEqualTo(2);
        assertDelete(updates.get(0), "order_number", 10101);
        assertTombstone(updates.get(1), "order_number", 10101);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-582")
    public void shouldEmitNoTombstoneOnDelete() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Testing.Print.enable();
        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT); // 6 DDL changes
        assertThat(records.recordsForTopic(DATABASE.topicForTable("orders")).size()).isEqualTo(5);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE orders SET order_number=10101 WHERE order_number=10001");
            }
        }
        // Consume the update of the PK, which is one insert followed by a delete...
        records = consumeRecordsByTopic(2);
        List<SourceRecord> updates = records.recordsForTopic(DATABASE.topicForTable("orders"));
        assertThat(updates.size()).isEqualTo(2);
        assertDelete(updates.get(0), "order_number", 10001);
        assertInsert(updates.get(1), "order_number", 10101);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("DELETE FROM orders WHERE order_number = 10101;");
                connection.execute("DELETE FROM orders WHERE order_number = 10002;");
            }
        }

        records = consumeRecordsByTopic(2);
        updates = records.recordsForTopic(DATABASE.topicForTable("orders"));
        assertThat(updates.size()).isEqualTo(2);
        assertDelete(updates.get(0), "order_number", 10101);
        assertDelete(updates.get(1), "order_number", 10002);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-794")
    public void shouldEmitNoSavepoints() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT); // 6 DDL changes
        assertThat(records.recordsForTopic(DATABASE.topicForTable("orders")).size()).isEqualTo(5);

        waitForStreamingRunning(DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                final Connection jdbc = connection.connection();
                connection.setAutoCommit(false);
                final Statement statement = jdbc.createStatement();
                // connection.executeWithoutCommitting("SavePoint mysavep2");
                statement.executeUpdate("DELETE FROM orders WHERE order_number = 10001");
                statement.executeUpdate("SavePoint sp2");
                statement.executeUpdate("DELETE FROM orders WHERE order_number = 10002");
                jdbc.commit();
            }
        }

        records = consumeRecordsByTopic(2);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName())).isNullOrEmpty();
        final List<SourceRecord> deletes = records.recordsForTopic(DATABASE.topicForTable("orders"));

        assertDelete(deletes.get(0), "order_number", 10001);
        assertDelete(deletes.get(1), "order_number", 10002);

        stopConnector();
    }

    /**
     * This test case validates that if you disable MySQL option binlog_rows_query_log_events or
     * the Maria option binlog_annotate_row_events, then the original SQL statement for an
     * INSERT statement is NOT parsed into the resulting event.
     */
    @Test
    @FixFor("DBZ-706")
    public void shouldNotParseQueryIfServerOptionDisabled() throws Exception {
        // Define the table we want to watch events from.
        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForStreamingRunning(DATABASE.getServerName());

        // Flush all existing records not related to the test.
        consumeRecords(PRODUCTS_TABLE_EVENT_COUNT, null);

        // Define insert query we want to validate.
        final String insertSqlStatement = "INSERT INTO products VALUES (default,'robot','Toy robot',1.304)";

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Disable Query log option
                db.setBinlogRowQueryEventsOff();

                // Execute insert statement.
                connection.execute(insertSqlStatement);
            }
        }

        // Lets see what gets produced?
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(tableName)).size()).isEqualTo(1);

        // Parse through the source record for the query value.
        final SourceRecord sourceRecord = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(0);

        // Should have been an insert with query parsed.
        validate(sourceRecord);
        assertInsert(sourceRecord, "id", 110);
        assertHasNoSourceQuery(sourceRecord);
    }

    /**
     * This test case validates that if you enable MySQL option binlog_rows_query_log_events
     * or the MariaDB option binlog_annotate_row_events but configure the connector to NOT
     * include the query, it will not be included in the event.
     */
    @Test
    @FixFor("DBZ-706")
    public void shouldNotParseQueryIfConnectorNotConfiguredTo() throws Exception {
        // Define the table we want to watch events from.
        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector to NOT parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, false)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Flush all existing records not related to the test.
        consumeRecords(PRODUCTS_TABLE_EVENT_COUNT, null);

        // Define insert query we want to validate.
        final String insertSqlStatement = "INSERT INTO products VALUES (default,'robot','Toy robot',1.304)";

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                // Execute insert statement.
                connection.execute(insertSqlStatement);
            }
        }

        // Lets see what gets produced?
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(tableName)).size()).isEqualTo(1);

        // Parse through the source record for the query value.
        final SourceRecord sourceRecord = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(0);
        logger.info("Record: {}", sourceRecord);

        // Should have been an insert with query parsed.
        validate(sourceRecord);
        assertInsert(sourceRecord, "id", 110);
        assertHasNoSourceQuery(sourceRecord);
    }

    /**
     * This test case validates that if you enable MySQL option binlog_rows_query_log_events or
     * the MariaDB option binlog_annotate_row_events, then the original SQL statement for an
     * INSERT statement is parsed into the resulting event.
     */
    @Test
    @FixFor("DBZ-706")
    public void shouldParseQueryIfAvailableAndConnectorOptionEnabled() throws Exception {
        // Define the table we want to watch events from.
        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Flush all existing records not related to the test.
        consumeRecords(PRODUCTS_TABLE_EVENT_COUNT, null);

        // Define insert query we want to validate.
        final String insertSqlStatement = "INSERT INTO products VALUES (default,'robot','Toy robot',1.304)";

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                // Execute insert statement.
                connection.execute(insertSqlStatement);
            }
        }

        // Lets see what gets produced?
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(tableName)).size()).isEqualTo(1);

        // Parse through the source record for the query value.
        final SourceRecord sourceRecord = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(0);
        logger.info("Record: {}", sourceRecord);

        // Should have been an insert with query parsed.
        validate(sourceRecord);
        assertInsert(sourceRecord, "id", 110);
        assertSourceQuery(sourceRecord, getExpectedQuery(insertSqlStatement));
    }

    /**
     * This test case validates that if you enable MySQL option binlog_rows_query_log_events or
     * the MariaDB option binlog_annotate_rows_event, then the issue multiple INSERTs, the
     * appropriate SQL statements are parsed into the resulting events.
     */
    @Test
    @FixFor("DBZ-706")
    public void parseMultipleInsertStatements() throws Exception {
        // Define the table we want to watch events from.
        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Flush all existing records not related to the test.
        consumeRecords(PRODUCTS_TABLE_EVENT_COUNT, null);

        // Define insert query we want to validate.
        final String insertSqlStatement1 = "INSERT INTO products VALUES (default,'robot','Toy robot',1.304)";
        final String insertSqlStatement2 = "INSERT INTO products VALUES (default,'toaster','Toaster',3.33)";

        logger.warn(DATABASE.getDatabaseName());

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                // Execute insert statement.
                connection.execute(insertSqlStatement1);
                connection.execute(insertSqlStatement2);
            }
        }

        // Lets see what gets produced?
        final SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(tableName)).size()).isEqualTo(2);

        // Parse through the source record for the query value.
        final SourceRecord sourceRecord1 = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(0);

        // Should have been an insert with query parsed.
        validate(sourceRecord1);
        assertInsert(sourceRecord1, "id", 110);
        assertSourceQuery(sourceRecord1, getExpectedQuery(insertSqlStatement1));

        // Grab second event
        final SourceRecord sourceRecord2 = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(1);

        // Should have been an insert with query parsed.
        validate(sourceRecord2);
        assertInsert(sourceRecord2, "id", 111);
        assertSourceQuery(sourceRecord2, getExpectedQuery(insertSqlStatement2));
    }

    /**
     * This test case validates that if you enable MySQL option binlog_rows_query_log_events or the
     * MariaDB option binlog_annotate_row_events, then the issue single multi-row INSERT, the
     * appropriate SQL statements are parsed into the resulting events.
     */
    @Test
    @FixFor("DBZ-706")
    public void parseMultipleRowInsertStatement() throws Exception {
        // Define the table we want to watch events from.
        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Flush all existing records not related to the test.
        consumeRecords(PRODUCTS_TABLE_EVENT_COUNT, null);

        // Define insert query we want to validate.
        final String insertSqlStatement = "INSERT INTO products VALUES (default,'robot','Toy robot',1.304), (default,'toaster','Toaster',3.33)";

        logger.warn(DATABASE.getDatabaseName());

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                // Execute insert statement.
                connection.execute(insertSqlStatement);
            }
        }

        // Lets see what gets produced?
        final SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(tableName)).size()).isEqualTo(2);

        // Parse through the source record for the query value.
        final SourceRecord sourceRecord1 = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(0);

        // Should have been an insert with query parsed.
        validate(sourceRecord1);
        assertInsert(sourceRecord1, "id", 110);
        assertSourceQuery(sourceRecord1, getExpectedQuery(insertSqlStatement));

        // Grab second event
        final SourceRecord sourceRecord2 = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(1);

        // Should have been an insert with query parsed.
        validate(sourceRecord2);
        assertInsert(sourceRecord2, "id", 111);
        assertSourceQuery(sourceRecord2, getExpectedQuery(insertSqlStatement));
    }

    /**
     * This test case validates that if you enable MySQL option binlog_rows_query_log_events or
     * the MariaDB option binlog_annotate_row_events, then the original SQL statement for a
     * DELETE over a single row is parsed into the resulting event.
     */
    @Test
    @FixFor("DBZ-706")
    public void parseDeleteQuery() throws Exception {
        // Define the table we want to watch events from.
        final String tableName = "orders";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Flush all existing records not related to the test.
        consumeRecords(ORDERS_TABLE_EVENT_COUNT, null);

        // Define insert query we want to validate.
        final String deleteSqlStatement = "DELETE FROM orders WHERE order_number=10001 LIMIT 1";

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                // Execute insert statement.
                connection.execute(deleteSqlStatement);
            }
        }

        // Lets see what gets produced?
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(tableName)).size()).isEqualTo(1);

        // Parse through the source record for the query value.
        final SourceRecord sourceRecord = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(0);

        // Should have been a delete with query parsed.
        validate(sourceRecord);
        assertDelete(sourceRecord, "order_number", 10001);
        assertSourceQuery(sourceRecord, getExpectedQuery(deleteSqlStatement));
    }

    /**
     * This test case validates that if you enable MySQL option binlog_rows_query_log_events or
     * the MariaDB option binlog_annotate_row_events, then issue a multi-row DELETE, the
     * resulting events get the original SQL statement.
     */
    @Test
    @FixFor("DBZ-706")
    public void parseMultiRowDeleteQuery() throws Exception {
        // Define the table we want to watch events from.
        final String tableName = "orders";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Flush all existing records not related to the test.
        consumeRecords(ORDERS_TABLE_EVENT_COUNT, null);

        // Define insert query we want to validate.
        final String deleteSqlStatement = "DELETE FROM orders WHERE purchaser=1002";

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                // Execute insert statement.
                connection.execute(deleteSqlStatement);
            }
        }

        // Lets see what gets produced?
        final SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(tableName)).size()).isEqualTo(2);

        // Parse through the source record for the query value.
        final SourceRecord sourceRecord1 = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(0);

        // Should have been a delete with query parsed.
        validate(sourceRecord1);
        assertDelete(sourceRecord1, "order_number", 10002);
        assertSourceQuery(sourceRecord1, getExpectedQuery(deleteSqlStatement));

        // Validate second event.
        final SourceRecord sourceRecord2 = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(1);

        // Should have been a delete with query parsed.
        validate(sourceRecord2);
        assertDelete(sourceRecord2, "order_number", 10004);
        assertSourceQuery(sourceRecord2, getExpectedQuery(deleteSqlStatement));
    }

    /**
     * This test case validates that if you enable MySQL option binlog_rows_query_log_events or
     * the MariaDB option binlog_annotate_row_events, then the original SQL statement for an
     * UPDATE over a single row is parsed into the resulting event.
     */
    @Test
    @FixFor("DBZ-706")
    public void parseUpdateQuery() throws Exception {
        // Define the table we want to watch events from.
        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Flush all existing records not related to the test.
        consumeRecords(PRODUCTS_TABLE_EVENT_COUNT, null);

        // Define insert query we want to validate.
        final String updateSqlStatement = "UPDATE products set name='toaster' where id=109 LIMIT 1";

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                // Execute insert statement.
                connection.execute(updateSqlStatement);
            }
        }

        // Lets see what gets produced?
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(tableName)).size()).isEqualTo(1);

        // Parse through the source record for the query value.
        final SourceRecord sourceRecord = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(0);

        // Should have been a delete with query parsed.
        validate(sourceRecord);
        assertUpdate(sourceRecord, "id", 109);
        assertSourceQuery(sourceRecord, getExpectedQuery(updateSqlStatement));
    }

    /**
     * This test case validates that if you enable MySQL option binlog_rows_query_log_events or
     * the MariaDB option binlog_annotate_row_events, then the original SQL statement for an
     * UPDATE over a single row is parsed into the resulting event.
     */
    @Test
    @FixFor("DBZ-706")
    public void parseMultiRowUpdateQuery() throws Exception {
        // Define the table we want to watch events from.
        final String tableName = "orders";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Flush all existing records not related to the test.
        consumeRecords(ORDERS_TABLE_EVENT_COUNT, null);

        // Define insert query we want to validate.
        final String updateSqlStatement = "UPDATE orders set quantity=0 where order_number in (10001, 10004)";

        // Connect to the DB and issue our insert statement to test.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // Enable Query log option
                db.setBinlogRowQueryEventsOn();

                // Execute insert statement.
                connection.execute(updateSqlStatement);
            }
        }

        // Lets see what gets produced?
        final SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(tableName)).size()).isEqualTo(2);

        // Parse through the source record for the query value.
        final SourceRecord sourceRecord1 = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(0);

        // Should have been a delete with query parsed.
        validate(sourceRecord1);
        assertUpdate(sourceRecord1, "order_number", 10001);
        assertSourceQuery(sourceRecord1, getExpectedQuery(updateSqlStatement));

        // Validate second event
        final SourceRecord sourceRecord2 = records.recordsForTopic(DATABASE.topicForTable(tableName)).get(1);

        // Should have been a delete with query parsed.
        validate(sourceRecord2);
        assertUpdate(sourceRecord2, "order_number", 10004);
        assertSourceQuery(sourceRecord2, getExpectedQuery(updateSqlStatement));
    }

    /**
     * Specifying the adaptive time.precision.mode is no longer valid and a configuration validation
     * problem should be reported when that configuration option is used.
     */
    @Test
    @FixFor("DBZ-1234")
    public void shouldFailToValidateAdaptivePrecisionMode() {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .with(BinlogConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE)
                .build();

        final Config result = validateConfiguration(config);
        assertConfigurationErrors(result, BinlogConnectorConfig.TIME_PRECISION_MODE);
    }

    @Test
    @FixFor("DBZ-1242")
    public void testEmptySchemaLogWarningWithDatabaseWhitelist() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, "my_database")
                .build();

        start(getConnectorClass(), config);

        consumeRecordsByTopic(12);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isTrue());
    }

    @Test
    @FixFor("DBZ-1242")
    public void testNoEmptySchemaLogWarningWithDatabaseWhitelist() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(getConnectorClass(), config);

        consumeRecordsByTopic(12);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse());
    }

    @Test
    @FixFor("DBZ-1242")
    public void testEmptySchemaWarningWithTableWhitelist() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        config = DATABASE.defaultConfig()
                .with(CommonConnectorConfig.FAIL_ON_NO_TABLES, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("my_products"))
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        consumeRecordsByTopic(12);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isTrue());
    }

    @Test
    @FixFor("DBZ-1242")
    public void testNoEmptySchemaWarningWithTableWhitelist() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        consumeRecordsByTopic(12);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse());
    }

    @Test
    @FixFor("DBZ-1015")
    public void shouldRewriteIdentityKey() throws InterruptedException, SQLException {
        // Define the table we want to watch events from.
        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                // rewrite key from table 'products': from {id} to {id, name}
                .with(BinlogConnectorConfig.MSG_KEY_COLUMNS, "(.*).products:id,name")
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        final SourceRecords records = consumeRecordsByTopic(9);
        // Parse through the source record for the query value.
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable(tableName));

        recordsForTopic.forEach(record -> {
            Struct key = (Struct) record.key();
            assertThat(key.get("id")).isNotNull();
            assertThat(key.get("name")).isNotNull();
        });
    }

    @Test
    @FixFor("DBZ-2957")
    public void shouldRewriteIdentityKeyWithWhitespace() throws InterruptedException, SQLException {
        // Define the table we want to watch events from.
        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                // Explicitly configure connector TO parse query
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                // rewrite key from table 'products': from {id} to {id, name}
                .with(BinlogConnectorConfig.MSG_KEY_COLUMNS, "   (.*).products:id,name   ")
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        final SourceRecords records = consumeRecordsByTopic(9);
        // Parse through the source record for the query value.
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable(tableName));

        recordsForTopic.forEach(record -> {
            Struct key = (Struct) record.key();
            assertThat(key.get("id")).isNotNull();
            assertThat(key.get("name")).isNotNull();
        });
    }

    @Test
    @FixFor("DBZ-2957")
    public void shouldRewriteIdentityKeyWithMsgKeyColumnsFieldRegexValidation() throws InterruptedException, SQLException {
        // Define the table we want to watch events from.
        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .with(BinlogConnectorConfig.MSG_KEY_COLUMNS, "(.*).products:id,name;")
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        final SourceRecords records = consumeRecordsByTopic(9);
        // Parse through the source record for the query value.
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable(tableName));

        recordsForTopic.forEach(record -> {
            Struct key = (Struct) record.key();
            assertThat(key.get("id")).isNotNull();
            assertThat(key.get("name")).isNotNull();
        });

    }

    @Test
    @FixFor("DBZ-1292")
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        Files.delete(SCHEMA_HISTORY_PATH);

        final String tableName = "products";

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(tableName))
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        final SourceRecords records = consumeRecordsByTopic(PRODUCTS_TABLE_EVENT_COUNT);
        final List<SourceRecord> table = records.recordsForTopic(DATABASE.topicForTable(tableName));

        for (SourceRecord record : table) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, getConnectorName(), "myServer1", false);
        }
    }

    private void waitForStreamingRunning(String serverName) throws InterruptedException {
        waitForStreamingRunning(getConnectorName(), serverName, getStreamingNamespace());
    }

    private List<SourceRecord> recordsForTopicForRoProductsTable(SourceRecords records) {
        final List<SourceRecord> uc = records.recordsForTopic(RO_DATABASE.topicForTable("Products"));
        return uc != null ? uc : records.recordsForTopic(RO_DATABASE.topicForTable("products"));
    }

    @Test
    @FixFor("DBZ-1531")
    public void shouldEmitHeadersOnPrimaryKeyUpdate() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT); // 6 DDL changes
        assertThat(records.recordsForTopic(DATABASE.topicForTable("orders")).size()).isEqualTo(5);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE orders SET order_number=10303 WHERE order_number=10003");
            }
        }
        // Consume the update of the PK, which is one insert followed by a delete followed by a tombstone ...
        records = consumeRecordsByTopic(3);
        List<SourceRecord> updates = records.recordsForTopic(DATABASE.topicForTable("orders"));

        assertThat(updates.size()).isEqualTo(3);

        SourceRecord deleteRecord = updates.get(0);
        Header keyPKUpdateHeader = getPKUpdateNewKeyHeader(deleteRecord).get();
        assertEquals(Integer.valueOf(10303), ((Struct) keyPKUpdateHeader.value()).getInt32("order_number"));

        SourceRecord insertRecord = updates.get(2);
        keyPKUpdateHeader = getPKUpdateOldKeyHeader(insertRecord).get();
        assertEquals(Integer.valueOf(10003), ((Struct) keyPKUpdateHeader.value()).getInt32("order_number"));

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE orders SET quantity=5 WHERE order_number=10004");
            }
        }
        records = consumeRecordsByTopic(1);
        updates = records.recordsForTopic(DATABASE.topicForTable("orders"));
        assertThat(updates.size()).isEqualTo(1);

        SourceRecord updateRecord = updates.get(0);
        assertThat(getPKUpdateNewKeyHeader(updateRecord).isPresent()).isFalse();

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1895")
    public void shouldEmitNoEventsForSkippedCreateOperations() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.SKIPPED_OPERATIONS, "c")
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO products VALUES (201,'rubberduck','Rubber Duck',2.12);");
                connection.execute("UPDATE products SET weight=3.13 WHERE name = 'rubberduck'");
                connection.execute("INSERT INTO products VALUES (202,'rubbercrocodile','Rubber Crocodile',4.14);");
                connection.execute("DELETE FROM products WHERE name = 'rubberduck'");
                connection.execute("INSERT INTO products VALUES (203,'rubberfish','Rubber Fish',5.15);");
                connection.execute("DELETE FROM products WHERE name = 'rubbercrocodile'");
                connection.execute("DELETE FROM products WHERE name = 'rubberfish'");
            }
        }

        SourceRecords records = consumeRecordsByTopic(7);
        List<SourceRecord> changeEvents = records.recordsForTopic(DATABASE.topicForTable("products"));

        assertUpdate(changeEvents.get(0), "id", 201);
        assertDelete(changeEvents.get(1), "id", 201);
        assertTombstone(changeEvents.get(2), "id", 201);
        assertDelete(changeEvents.get(3), "id", 202);
        assertTombstone(changeEvents.get(4), "id", 202);
        assertDelete(changeEvents.get(5), "id", 203);
        assertTombstone(changeEvents.get(6), "id", 203);
        assertThat(changeEvents.size()).isEqualTo(7);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1895")
    public void shouldEmitNoEventsForSkippedUpdateAndDeleteOperations() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.SKIPPED_OPERATIONS, "u,d")
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO products VALUES (204,'rubberduck','Rubber Duck',2.12);");
                connection.execute("UPDATE products SET weight=3.13 WHERE name = 'rubberduck'");
                connection.execute("INSERT INTO products VALUES (205,'rubbercrocodile','Rubber Crocodile',4.14);");
                connection.execute("DELETE FROM products WHERE name = 'rubberduck'");
                connection.execute("INSERT INTO products VALUES (206,'rubberfish','Rubber Fish',5.15);");
            }
        }

        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> changeEvents = records.recordsForTopic(DATABASE.topicForTable("products"));

        assertInsert(changeEvents.get(0), "id", 204);
        assertInsert(changeEvents.get(1), "id", 205);
        assertInsert(changeEvents.get(2), "id", 206);
        assertThat(changeEvents.size()).isEqualTo(3);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1344")
    public void testNoEmptySchemaLogWarningWithSnapshotNever() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseSchema.class);

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, "my_database")
                .build();

        start(getConnectorClass(), config);

        consumeRecordsByTopic(12);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse());
    }

    @Test
    public void shouldNotUseOffsetWhenSnapshotIsAlways() throws Exception {

        try {
            config = DATABASE.defaultConfig()
                    .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS)
                    .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("always_snapshot"))
                    .with(BinlogConnectorConfig.SNAPSHOT_MODE_TABLES, DATABASE.qualifiedTableName("always_snapshot"))
                    .with(BinlogConnectorConfig.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                    .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                    .build();

            try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
                try (JdbcConnection connection = db.connect()) {
                    connection.execute(String.format("CREATE TABLE %s.`always_snapshot` ("
                            + " id INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,"
                            + " `data` VARCHAR(50) NOT NULL);", DATABASE.getDatabaseName()));

                    connection.execute(String.format("INSERT INTO %s.`always_snapshot` VALUES (1,'Test1');", DATABASE.getDatabaseName()));
                    connection.execute(String.format("INSERT INTO %s.`always_snapshot` VALUES (2,'Test2');", DATABASE.getDatabaseName()));
                }
            }

            start(getConnectorClass(), config);
            waitForStreamingRunning(DATABASE.getServerName());

            int expectedRecordCount = 2;
            SourceRecords sourceRecords = consumeRecordsByTopic(expectedRecordCount);
            assertThat(sourceRecords.recordsForTopic(DATABASE.topicForTable("always_snapshot"))).hasSize(expectedRecordCount);
            Struct struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(0).value()).get(AFTER);
            assertEquals(1, struct.get("id"));
            assertEquals("Test1", struct.get("data"));
            struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(1).value()).get(AFTER);
            assertEquals(2, struct.get("id"));
            assertEquals("Test2", struct.get("data"));

            stopConnector();

            try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
                try (JdbcConnection connection = db.connect()) {

                    connection.execute(String.format("DELETE FROM %s.`always_snapshot` WHERE id=1;", DATABASE.getDatabaseName()));
                    connection.execute(String.format("INSERT INTO %s.`always_snapshot` VALUES (3,'Test3');", DATABASE.getDatabaseName()));
                }
            }

            start(getConnectorClass(), config);
            waitForStreamingRunning(DATABASE.getServerName());
            sourceRecords = consumeRecordsByTopic(expectedRecordCount);

            // Check we get up-to-date data in the snapshot.
            assertThat(sourceRecords.recordsForTopic(DATABASE.topicForTable("always_snapshot"))).hasSize(expectedRecordCount);
            struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(0).value()).get(AFTER);
            assertEquals(2, struct.get("id"));
            assertEquals("Test2", struct.get("data"));
            struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(1).value()).get(AFTER);
            assertEquals(3, struct.get("id"));
            assertEquals("Test3", struct.get("data"));
        }
        finally {
            try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
                try (JdbcConnection connection = db.connect()) {

                    connection.execute(String.format("DROP TABLE %s.`always_snapshot` ", DATABASE.getDatabaseName()));
                }
            }
        }
    }

    @Test
    @FixFor("DBZ-3949")
    public void testDmlInChangeEvents() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("products"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE, CommonConnectorConfig.EventProcessingFailureHandlingMode.FAIL)
                .build();

        // Start the connector.
        CompletionResult completion = new CompletionResult();
        start(getConnectorClass(), config, completion);
        waitForStreamingRunning(DATABASE.getServerName());

        // Do some changes.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO products VALUES (204,'rubberduck','Rubber Duck',2.12);");
                connection.execute("INSERT INTO products VALUES (205,'rubbercrocodile','Rubber Crocodile',4.14);");
                connection.execute("INSERT INTO products VALUES (206,'rubberfish','Rubber Fish',5.15);");
            }
        }

        // Switch to 'STATEMENT' binlog format to mimic DML events in the log.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(String.format("SET GLOBAL binlog_format = 'STATEMENT'", DATABASE.getDatabaseName()));
            }
        }

        // Do some more changes.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("UPDATE products SET weight=2.22 WHERE id=204;");
                connection.execute("UPDATE products SET weight=4.44 WHERE id=205;");
                connection.execute("UPDATE products SET weight=5.55 WHERE id=206;");
            }
        }

        // Last 3 changes should be ignored as they were stored using STATEMENT format.
        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> changeEvents = records.recordsForTopic(DATABASE.topicForTable("products"));
        assertThat(changeEvents.size()).isEqualTo(3);
        // There shouldn't be any error.
        assertThat(completion.hasError()).isFalse();

        // Switch back to 'ROW' binlog format.
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(String.format("SET GLOBAL binlog_format = 'ROW'", DATABASE.getDatabaseName()));
            }
        }

        stopConnector();
    }

    @Test
    @FixFor("DBZ-5052")
    public void shouldNotSendTombstonesWhenNotSupportedByHandler() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.SKIPPED_OPERATIONS, "c")
                .build();

        start(getConnectorClass(), config, new NoTombStonesHandler(consumedLines));
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO products VALUES (201,'rubberduck','Rubber Duck',2.12);");
                connection.execute("DELETE FROM products WHERE name = 'rubberduck'");
                connection.execute("INSERT INTO products VALUES (201,'rubberduck','Rubber Duck',2.12);");
                connection.execute("DELETE FROM products WHERE name = 'rubberduck'");
            }
        }

        // INSERT is excluded, DELETE generates delete and tombstone event, but tombstones shouldn't be produced, thus 2 events overall.
        SourceRecords records = consumeRecordsByTopic(2);
        List<SourceRecord> changeEvents = records.recordsForTopic(DATABASE.topicForTable("products"));

        assertDelete(changeEvents.get(0), "id", 201);
        assertDelete(changeEvents.get(1), "id", 201);
        assertThat(changeEvents.size()).isEqualTo(2);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-5610")
    public void shouldEmitTruncateOperation() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(getSnapshotLockingModeField(), getSnapshotLockingModeNone())
                .with(BinlogConnectorConfig.SKIPPED_OPERATIONS, "none")
                .build();

        start(getConnectorClass(), config);
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into orders values(1000, '2022-10-09', 1002, 90, 106)");
                connection.execute("truncate table orders;");
            }
        }

        SourceRecords records = consumeRecordsByTopic(2);
        List<SourceRecord> changeEvents = records.recordsForTopic(DATABASE.topicForTable("orders"));
        Struct truncateStruct = (Struct) changeEvents.get(1).value();

        assertInsert(changeEvents.get(0), "order_number", 1000);
        assertThat(truncateStruct.get("before")).isNull();
        assertThat(truncateStruct.get("after")).isNull();
        assertThat(truncateStruct.get("op")).isEqualTo("t");
        assertThat(changeEvents.size()).isEqualTo(2);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-8134")
    public void shouldAcceptLongAsServerId() throws InterruptedException {
        Configuration config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SERVER_ID, "202309181059")
                .build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(DATABASE.getServerName());
        stopConnector();
    }

    @Test
    @FixFor("DBZ-8290")
    public void shouldUpdateTotalRecordsCounter() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(BinlogStreamingChangeEventSource.class);
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(getConnectorClass(), config);
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into orders values(1000, '2022-10-09', 1002, 90, 106)");
                connection.commit();
            }
        }

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> changeEvents = records.recordsForTopic(DATABASE.topicForTable("orders"));
        assertThat(changeEvents.size()).isEqualTo(1);

        // Here we count all the records obtained from binlog, not only records pushed into the sink.
        // Number of records may vary between MySQL and MariaDB and also between the runs on the same DB.
        stopConnector(value -> assertThat(logInterceptor.messageMatches("^Stopped reading binlog after [5-9] events(.*)")).isTrue());
    }

    protected String getExpectedQuery(String statement) {

        return statement;
    }

    private static class NoTombStonesHandler implements DebeziumEngine.ChangeConsumer<SourceRecord> {
        protected BlockingQueue<SourceRecord> recordQueue;

        NoTombStonesHandler(BlockingQueue<SourceRecord> recordQueue) {
            this.recordQueue = recordQueue;
        }

        public void handleBatch(List<SourceRecord> records, DebeziumEngine.RecordCommitter<SourceRecord> committer) throws InterruptedException {
            for (SourceRecord r : records) {
                recordQueue.offer(r);
                committer.markProcessed(r);
            }
        }

        public boolean supportsTombstoneEvents() {
            return false;
        }
    }
}
