/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.Map;
import java.util.function.Supplier;

import org.fest.assertions.Assertions;
import org.fest.assertions.MapAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.ShouldFailWhen;
import io.debezium.util.Testing;

@ShouldFailWhen(ReadOnlyIncrementalSnapshotIT.IsGtidModeOff.class)
public class ReadOnlyIncrementalSnapshotIT extends IncrementalSnapshotIT {

    public static final String EXCLUDED_TABLE = "b";
    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    protected Configuration.Builder config() {
        return super.config()
                .with(MySqlConnectorConfig.TABLE_EXCLUDE_LIST, DATABASE.getDatabaseName() + "." + EXCLUDED_TABLE)
                .with(MySqlConnectorConfig.READ_ONLY_CONNECTION, true);
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

    public static class IsGtidModeOff implements Supplier<Boolean> {

        public Boolean get() {
            try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase("emptydb")) {
                return db.queryAndMap(
                        "SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'",
                        rs -> {
                            if (rs.next()) {
                                return "OFF".equalsIgnoreCase(rs.getString(2));
                            }
                            throw new IllegalStateException("Cannot obtain GTID status");
                        });
            }
            catch (SQLException e) {
                throw new IllegalStateException("Cannot obtain GTID status", e);
            }
        }
    }
}
