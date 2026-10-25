/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.JdbcDatabaseContainer;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Testing;

/**
 * Verifies that database and table names follow the server's {@code lower_case_table_names} setting, whether a table is
 * known from a DDL statement or from binlog {@code TABLE_MAP} events.
 * <p>
 * By default the server runs with {@code lower_case_table_names=1}, which also works on case-sensitive file systems such as
 * the ones used by CI. With this setting, DDL statements keep the case they were written in while the server and
 * {@code TABLE_MAP} events use lowercase names. Setting the {@code lower.case.table.names} system property to {@code 2} runs
 * the same tests with a data directory bind-mounted from the host, which requires a case-insensitive host file system, such
 * as the default one on macOS. The tests are skipped when the server does not apply the requested setting.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BinlogLowerCaseTableNamesIT<C extends SourceConnector>
        extends AbstractAsyncEngineConnectorTest
        implements BinlogConnectorTest<C> {

    protected static final String ROOT_PASSWORD = "debezium";

    private static final String SERVER_NAME = "lctn_server";
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-lctn.txt").toAbsolutePath();

    private final int lowerCaseTableNames = Integer.getInteger("lower.case.table.names", 1);
    private JdbcDatabaseContainer<?> container;

    /**
     * Creates the database server container, which is started by this test.
     *
     * @param serverOptions options to pass to the server; never null
     * @return the container; never null
     */
    protected abstract JdbcDatabaseContainer<?> createContainer(List<String> serverOptions);

    /**
     * Adds the connector-specific connection settings for the database server container.
     *
     * @param builder the connector configuration builder; never null
     * @return the builder; never null
     */
    protected abstract Configuration.Builder configureConnection(Configuration.Builder builder);

    @BeforeAll
    void startDatabase() throws Exception {
        final List<String> serverOptions = new ArrayList<>();
        serverOptions.add("--lower-case-table-names=" + lowerCaseTableNames);
        container = createContainer(serverOptions);
        if (lowerCaseTableNames == 2) {
            final File dataDirectory = Testing.Files.createTestingDirectory("lctn-data-" + getConnectorName(), true);
            container.withFileSystemBind(dataDirectory.getAbsolutePath(), "/var/lib/mysql", BindMode.READ_WRITE);
        }
        container.start();

        try (var connection = container.createConnection("");
                var statement = connection.createStatement();
                var resultSet = statement.executeQuery("SELECT @@lower_case_table_names")) {
            resultSet.next();
            assumeTrue(resultSet.getInt(1) == lowerCaseTableNames,
                    "The server does not run with lower_case_table_names=" + lowerCaseTableNames);
        }
    }

    @AfterAll
    void stopDatabase() {
        if (container != null) {
            container.stop();
        }
    }

    @BeforeEach
    void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        stopConnector();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @Test
    @FixFor("debezium/dbz#125")
    public void shouldUseServerNamesForTablesCreatedWhileStreaming() throws Exception {
        final String database = "LctnNames";
        final Configuration config = config(database)
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), SERVER_NAME);

        // One table is named with the database, the other one is resolved through the session database
        execute(null,
                "CREATE DATABASE " + database,
                "CREATE TABLE " + database + ".Qualified (id INT PRIMARY KEY, name VARCHAR(30))");
        execute(database, "CREATE TABLE Unqualified (id INT PRIMARY KEY, name VARCHAR(30))");
        executeInTransaction(database,
                "INSERT INTO Qualified VALUES (1, 'created')",
                "INSERT INTO UNQUALIFIED VALUES (1, 'created')");
        execute(database,
                "UPDATE qualified SET name = 'updated' WHERE id = 1",
                "DELETE FROM QUALIFIED WHERE id = 1");

        // 4 data events and the BEGIN and END events of 3 transactions
        final SourceRecords records = consumeRecordsByTopic(10);
        final List<SourceRecord> qualified = records.recordsForTopic(topicName(database, "Qualified"));
        assertThat(qualified).extracting(this::operation).containsExactly("c", "u", "d");
        assertThat(records.recordsForTopic(topicName(database, "Unqualified"))).hasSize(1);
        assertSourceAndSchemaNames(qualified, database, "Qualified");
        assertSourceAndSchemaNames(records.recordsForTopic(topicName(database, "Unqualified")), database, "Unqualified");

        final List<String> dataCollections = records.recordsForTopic(SERVER_NAME + ".transaction").stream()
                .map(record -> (Struct) record.value())
                .filter(value -> "END".equals(value.getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY)))
                .flatMap(value -> value.getArray(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTIONS_KEY).stream())
                .map(dataCollection -> ((Struct) dataCollection).getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_COLLECTION_KEY))
                .collect(Collectors.toList());
        assertThat(dataCollections).containsOnly(dataCollectionName(database, "Qualified"), dataCollectionName(database, "Unqualified"));

        // The names recovered from the schema history after a restart are the same
        stopConnector();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), SERVER_NAME);
        execute(database, "INSERT INTO Qualified VALUES (2, 'after restart')", "INSERT INTO Unqualified VALUES (2, 'after restart')");

        final SourceRecords recordsAfterRestart = consumeRecordsByTopic(6);
        assertThat(recordsAfterRestart.recordsForTopic(topicName(database, "Qualified"))).hasSize(1);
        assertThat(recordsAfterRestart.recordsForTopic(topicName(database, "Unqualified"))).hasSize(1);
        assertSourceAndSchemaNames(recordsAfterRestart.recordsForTopic(topicName(database, "Qualified")), database, "Qualified");
    }

    @Test
    @FixFor("debezium/dbz#125")
    public void shouldMatchSignalsAndIncrementalSnapshotsRegardlessOfCase() throws Exception {
        final String database = "LctnSignals";
        // The signal table and the snapshot data collection are configured in a different case than they are created with
        final Configuration config = config(database)
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, (database + ".Dbz_Signal").toUpperCase(Locale.ROOT))
                .with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 2)
                .build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), SERVER_NAME);

        execute(null,
                "CREATE DATABASE " + database,
                "CREATE TABLE " + database + ".Dbz_Signal (id VARCHAR(64) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048))",
                "CREATE TABLE " + database + ".Member (id INT PRIMARY KEY, name VARCHAR(30))",
                "INSERT INTO " + database + ".Member VALUES (1, 'one'), (2, 'two'), (3, 'three')");
        assertThat(consumeRecordsByTopic(3).recordsForTopic(topicName(database, "Member"))).hasSize(3);

        execute(database, String.format(
                "INSERT INTO Dbz_Signal VALUES ('ad-hoc-1', 'execute-snapshot', '{\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}')",
                (database + ".Member").toUpperCase(Locale.ROOT)));

        // The snapshot also writes watermarks to the signal table, so read until the last row of the snapshot
        final String memberTopic = topicName(database, "Member");
        final SourceRecords records = consumeRecordsByTopicUntil((count, record) -> memberTopic.equals(record.topic())
                && "r".equals(operation(record)) && ((Struct) record.key()).getInt32("id") == 3);
        final List<SourceRecord> snapshotRecords = records.recordsForTopic(memberTopic);
        assertThat(snapshotRecords).extracting(this::operation).containsExactly("r", "r", "r");
        assertSourceAndSchemaNames(snapshotRecords, database, "Member");
    }

    private Configuration.Builder config(String database) {
        return configureConnection(Configuration.create())
                .with(BinlogConnectorConfig.HOSTNAME, container.getHost())
                .with(BinlogConnectorConfig.PORT, container.getMappedPort(3306))
                .with(BinlogConnectorConfig.USER, "root")
                .with(BinlogConnectorConfig.PASSWORD, ROOT_PASSWORD)
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, database)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH);
    }

    private void execute(String database, String... statements) throws SQLException {
        try (var connection = container.createConnection("");
                var statement = connection.createStatement()) {
            if (database != null) {
                statement.execute("USE " + database);
            }
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private void executeInTransaction(String database, String... statements) throws SQLException {
        try (var connection = container.createConnection("");
                var statement = connection.createStatement()) {
            statement.execute("USE " + database);
            connection.setAutoCommit(false);
            for (String sql : statements) {
                statement.execute(sql);
            }
            connection.commit();
        }
    }

    private void assertSourceAndSchemaNames(List<SourceRecord> records, String database, String table) {
        assertThat(records).isNotEmpty().allSatisfy(record -> {
            final Struct source = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
            assertThat(source.getString("db")).isEqualTo(serverName(database));
            assertThat(source.getString("table")).isEqualTo(serverName(table));
            assertThat(record.keySchema().name()).isEqualTo(topicName(database, table) + ".Key");
            assertThat(record.valueSchema().name()).isEqualTo(topicName(database, table) + ".Envelope");
        });
    }

    private String operation(SourceRecord record) {
        return ((Struct) record.value()).getString(Envelope.FieldName.OPERATION);
    }

    private String topicName(String database, String table) {
        return SERVER_NAME + "." + dataCollectionName(database, table);
    }

    private String dataCollectionName(String database, String table) {
        return serverName(database) + "." + serverName(table);
    }

    /**
     * Returns the name the server reports for a database or table created with the given name.
     */
    private String serverName(String createdName) {
        return lowerCaseTableNames == 1 ? createdName.toLowerCase(Locale.ROOT) : createdName;
    }
}
