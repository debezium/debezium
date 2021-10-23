/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.apache.kafka.connect.data.Schema.Type.INT32;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;

public abstract class AbstractIncrementalSnapshotWithSchemaChangesSupportTest<T extends SourceConnector> extends AbstractIncrementalSnapshotTest<T> {

    protected abstract String tableName(String table);

    protected abstract String alterColumnStatement(String table, String column, String type);

    protected abstract String alterColumnSetNotNullStatement(String table, String column, String type);

    protected abstract String alterColumnDropNotNullStatement(String table, String column, String type);

    protected abstract String alterColumnSetDefaultStatement(String table, String column, String type, String defaultValue);

    protected abstract String alterColumnDropDefaultStatement(String table, String column, String type);

    protected abstract void executeRenameTable(JdbcConnection connection, String newTable) throws SQLException;

    protected abstract String createTableStatement(String newTable, String copyTable);

    @Test
    public void schemaChanges() throws Exception {
        Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            connection.execute(String.format("INSERT INTO %s (pk, aa) VALUES (%s, %s)", tableName(), ROW_COUNT + 11, ROW_COUNT + 10));
            connection.execute(alterColumnStatement(tableName(), "aa", "VARCHAR(5)"));
            connection.execute(String.format("INSERT INTO %s (pk, aa) VALUES (%s, '%s')", tableName(), ROW_COUNT + 10, ROW_COUNT + 9));
            for (int i = 0; i < 9 && !Thread.interrupted(); i += 3) {
                connection.execute(String.format("ALTER TABLE %s ADD c INT", tableName()));
                connection.execute(String.format("INSERT INTO %s (pk, aa, c) VALUES (%s, '%s', %s)", tableName(), i + ROW_COUNT + 1, i + ROW_COUNT, 1));
                connection.execute(alterColumnStatement(tableName(), "c", "VARCHAR(5)"));
                connection.execute(String.format("INSERT INTO %s (pk, aa, c) VALUES (%s, '%s', '%s')", tableName(), i + ROW_COUNT + 2, i + ROW_COUNT + 1, "1"));
                connection.execute(String.format("ALTER TABLE %s DROP COLUMN c", tableName()));
                connection.execute(String.format("INSERT INTO %s (pk, aa) VALUES (%s, '%s')", tableName(), i + ROW_COUNT + 3, i + ROW_COUNT + 2));
            }
        }

        final int expectedRecordCount = ROW_COUNT + 11;
        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertTrue(String.format("missing PK %d", i + 1), dbChanges.containsKey(i + 1));
            SourceRecord record = dbChanges.get(i + 1);
            final Schema.Type valueType = record.valueSchema().field("after").schema().field(valueFieldName()).schema().type();
            if (valueType == INT32) {
                final int value = ((Struct) record.value()).getStruct("after").getInt32(valueFieldName());
                assertEquals(i, value);
            }
            else {
                String value = ((Struct) record.value()).getStruct("after").getString(valueFieldName());
                assertEquals(Integer.toString(i), value);
            }
        }
    }

    @Test
    public void renameTable() throws Exception {
        Print.enable();

        populateTable();
        final String newTable = "new_table";
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            connection.execute(createTableStatement(tableName(newTable), tableName()));
            connection.execute(String.format("INSERT INTO %s SELECT * FROM %s", tableName(newTable), tableName()));
            connection.execute(String.format("ALTER TABLE %s ADD c varchar(5)", tableName(newTable)));
        }
        startConnector();
        sendAdHocSnapshotSignal();

        final int updatedRowCount = 10;
        final AtomicInteger recordCounter = new AtomicInteger();
        final AtomicBoolean tableRenamed = new AtomicBoolean();
        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(ROW_COUNT, x -> true,
                x -> {
                    if (recordCounter.addAndGet(x.size()) > 10 && !tableRenamed.get()) {
                        try (JdbcConnection connection = databaseConnection()) {
                            executeRenameTable(connection, newTable);
                            connection.executeWithoutCommitting(
                                    String.format("UPDATE %s SET c = 'c' WHERE pk >= %s", tableName(), ROW_COUNT - updatedRowCount));
                            connection.commit();
                        }
                        catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        tableRenamed.set(true);
                    }
                });
        for (int i = 0; i < ROW_COUNT - updatedRowCount; i++) {
            assertTrue(dbChanges.containsKey(i + 1));
            SourceRecord record = dbChanges.get(i + 1);
            final int value = ((Struct) record.value()).getStruct("after").getInt32(valueFieldName());
            assertEquals(i, value);
            if (((Struct) record.value()).schema().field("c") != null) {
                final String c = ((Struct) record.value()).getStruct("after").getString("c");
                assertNull(c);
            }
        }

        for (int i = ROW_COUNT - updatedRowCount; i < ROW_COUNT; i++) {
            assertTrue(dbChanges.containsKey(i + 1));
            SourceRecord record = dbChanges.get(i + 1);
            final int value = ((Struct) record.value()).getStruct("after").getInt32(valueFieldName());
            assertEquals(i, value);
            final String c = ((Struct) record.value()).getStruct("after").getString("c");
            assertEquals("c", c);
        }
    }

    @Test
    public void columnNullabilityChanges() throws Exception {
        Print.enable();

        populateTable();
        final Configuration config = config().build();
        startAndConsumeTillEnd(connectorClass(), config);
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();
        sendAdHocSnapshotSignal();

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            connection.execute(alterColumnSetNotNullStatement(tableName(), "aa", "INTEGER"));
            connection.commit();

            connection.execute(alterColumnDropNotNullStatement(tableName(), "aa", "INTEGER"));
            connection.commit();

            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk, aa) VALUES (%s, null)",
                        tableName(),
                        i + ROW_COUNT + 1));
            }
            connection.commit();
        }
        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedRecordCount);

        for (int i = 0; i < ROW_COUNT; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final int value = ((Struct) record.value()).getStruct("after").getInt32(valueFieldName());
            assertEquals(i, value);
        }

        for (int i = ROW_COUNT; i < 2 * ROW_COUNT; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final Integer value = ((Struct) record.value()).getStruct("after").getInt32(valueFieldName());
            assertNull(value);
        }
    }

    @Test
    public void columnDefaultChanges() throws Exception {
        Print.enable();

        populateTable();
        final Configuration config = config().build();
        startAndConsumeTillEnd(connectorClass(), config);
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT * 4;
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            connection.execute(alterColumnSetDefaultStatement(tableName(), "aa", "INTEGER", "-6"));
            connection.commit();

            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk) VALUES (%s)",
                        tableName(),
                        i + ROW_COUNT + 1));
            }
            connection.commit();

            connection.executeWithoutCommitting(alterColumnDropDefaultStatement(tableName(), "aa", "INTEGER"));
            connection.executeWithoutCommitting(alterColumnSetDefaultStatement(tableName(), "aa", "INTEGER", "-9"));
            connection.commit();
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk) VALUES (%s)",
                        tableName(),
                        i + 2 * ROW_COUNT + 1));
            }
            connection.commit();

            connection.executeWithoutCommitting(alterColumnDropDefaultStatement(tableName(), "aa", "INTEGER"));
            connection.commit();
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk) VALUES (%s)",
                        tableName(),
                        i + 3 * ROW_COUNT + 1));
            }
            connection.commit();
        }
        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedRecordCount);

        for (int i = 0; i < ROW_COUNT; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final Struct after = ((Struct) record.value()).getStruct("after");
            final int value = after.getInt32(valueFieldName());
            assertEquals(i, value);
        }

        for (int i = ROW_COUNT; i < 2 * ROW_COUNT; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final Struct after = ((Struct) record.value()).getStruct("after");
            final Integer value = after.getInt32(valueFieldName());
            assertNotNull("value is null at pk=" + (i + 1), value);
            assertEquals(String.format("value is %d at pk = %d, expected -6", value, i + 1), -6, value, 0);
        }

        for (int i = 2 * ROW_COUNT; i < 3 * ROW_COUNT; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final Struct after = ((Struct) record.value()).getStruct("after");
            final Integer value = after.getInt32(valueFieldName());
            assertNotNull("value is null at pk=" + (i + 1), value);
            assertEquals(String.format("value is %d at pk = %d, expected -9", value, i + 1), -9, value, 0);
        }

        for (int i = 3 * ROW_COUNT; i < 4 * ROW_COUNT; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final Struct after = ((Struct) record.value()).getStruct("after");
            final Integer value = after.getInt32(valueFieldName());
            assertNull("value is not null at pk=" + (i + 1), value);
        }
    }

}
