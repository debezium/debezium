/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotLockingMode;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Verifies that {@link RetryOnTableDefinitionChangedExtension}, which is registered automatically for the module,
 * re-executes a test whose connector stopped with {@code ORA-01466} and re-runs the test lifecycle methods first.
 * <p>
 * The first attempt deliberately changes a captured table's definition while the snapshot is in progress with
 * table locking disabled, which makes the connector fail with {@code ORA-01466}. The second attempt does not,
 * so the test passes only when it is re-executed by the extension.
 *
 * @author Chris Cranford
 */
public class RetryOnTableDefinitionChangedExtensionIT extends AbstractAsyncEngineConnectorTest {

    private static final AtomicInteger ATTEMPTS = new AtomicInteger();
    private static final AtomicInteger SETUPS = new AtomicInteger();
    private static final AtomicInteger TEARDOWNS = new AtomicInteger();

    private static final int ROW_COUNT = 2000;

    private OracleConnection connection;

    @BeforeEach
    public void before() throws SQLException {
        SETUPS.incrementAndGet();

        connection = TestHelper.testConnection();
        TestHelper.dropTables(connection, "dbz2787a", "dbz2787b");

        // Table "a" holds more rows than the test's record buffer plus the connector queue can hold, so that the
        // snapshot blocks on back pressure while reading it; table "b" is snapshotted afterwards and is the
        // table whose definition is changed, so its flashback query at the snapshot SCN fails with ORA-01466
        connection.execute("CREATE TABLE dbz2787a (id NUMERIC(9,0) NOT NULL, data VARCHAR2(50), PRIMARY KEY (id))");
        connection.execute("CREATE TABLE dbz2787b (id NUMERIC(9,0) NOT NULL, data VARCHAR2(50), PRIMARY KEY (id))");
        TestHelper.streamTable(connection, "dbz2787a");
        TestHelper.streamTable(connection, "dbz2787b");
        for (int i = 1; i <= ROW_COUNT; i++) {
            connection.executeWithoutCommitting("INSERT INTO dbz2787a VALUES (" + i + ", 'row " + i + "')");
        }
        connection.executeWithoutCommitting("INSERT INTO dbz2787b VALUES (1, 'row 1')");
        connection.commit();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    public void after() throws SQLException {
        TEARDOWNS.incrementAndGet();
        if (connection != null) {
            TestHelper.dropTables(connection, "dbz2787a", "dbz2787b");
            connection.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#2787")
    public void shouldReExecuteTestWhenConnectorStopsWithTableDefinitionChanged() throws Exception {
        final int attempt = ATTEMPTS.incrementAndGet();

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(OracleConnectorConfig.SNAPSHOT_LOCKING_MODE, SnapshotLockingMode.NONE)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2787[AB]")
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 2)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, 4)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        // Once the record buffer is full the snapshot is reading table "a" and cannot proceed to table "b"
        // until the records are consumed
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> consumedLines.remainingCapacity() == 0);

        if (attempt == 1) {
            // No table locks are held, so the DDL succeeds and invalidates the snapshot SCN for table "b"
            try (OracleConnection otherSession = TestHelper.testConnection()) {
                otherSession.execute("ALTER TABLE dbz2787b MODIFY (data VARCHAR2(100))");
            }
        }

        final SourceRecords records = consumeRecordsByTopic(ROW_COUNT + 1);
        assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ2787A")).hasSize(ROW_COUNT);
        // Fails on the first attempt, as the connector stopped with ORA-01466 before reading table "b"
        assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ2787B")).hasSize(1);
        assertConnectorIsRunning();

        // The extension re-executed the test, including its lifecycle methods
        assertThat(attempt).isEqualTo(2);
        assertThat(SETUPS.get()).isEqualTo(2);
        assertThat(TEARDOWNS.get()).isEqualTo(1);
    }
}
