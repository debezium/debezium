/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.fest.assertions.Assertions;
import org.fest.assertions.MapAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.junit.SkipTestDependingOnGtidModeRule;
import io.debezium.connector.mysql.junit.SkipWhenGtidModeIs;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

@SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF, reason = "Read only connection requires GTID_MODE to be ON")
public class ReadOnlyIncrementalSnapshotIT extends IncrementalSnapshotIT {

    public static final String EXCLUDED_TABLE = "b";
    @Rule
    public TestRule skipTest = new SkipTestDependingOnGtidModeRule();

    protected Configuration.Builder config() {
        return super.config()
                .with(MySqlConnectorConfig.TABLE_EXCLUDE_LIST, DATABASE.getDatabaseName() + "." + EXCLUDED_TABLE)
                .with(MySqlConnectorConfig.READ_ONLY_CONNECTION, true);
    }

    @Test
    public void emptyHighWatermark() throws Exception {
        Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void filteredEvents() throws Exception {
        Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        Thread t = new Thread(() -> {
            try (JdbcConnection connection = databaseConnection()) {
                connection.setAutoCommit(false);
                for (int i = 0; true; i++) {
                    connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk, aa) VALUES (%s, %s)",
                            EXCLUDED_TABLE,
                            i + ROW_COUNT + 1,
                            i + ROW_COUNT));
                    connection.commit();
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        t.setDaemon(true);
        t.setName("filtered-binlog-events-thread");
        t.start();

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test(expected = ConnectException.class)
    @SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.ON, reason = "Read only connection requires GTID_MODE to be ON")
    public void shouldFailIfGtidModeIsOff() throws Exception {
        Testing.Print.enable();
        populateTable();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        startConnector((success, message, error) -> exception.set(error));
        waitForConnectorShutdown("mysql", DATABASE.getServerName());
        stopConnector();
        final Throwable e = exception.get();
        if (e != null) {
            throw (RuntimeException) e;
        }
    }
}
