/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.apache.kafka.connect.data.Schema.Type.INT32;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

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
        // Testing.Print.enable();

        populateTable();
        startConnector();
        executeSchemaChangesTest(false);
    }

    @Test
    @FixFor("debezium/dbz#2452")
    public void schemaChangesWithColumnFilter() throws Exception {
        // Testing.Print.enable();

        // A non-matching exclude list keeps every column but switches the chunk query from '*' to
        // an explicit column projection, so a DDL landing mid-snapshot can also surface as an
        // undefined-column error on the query itself instead of only as a result-set mismatch.
        populateTable();
        final var logInterceptor = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);
        startConnector(x -> x.with(RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST, "no_such_schema\\.no_such_table\\.no_such_column"));
        executeSchemaChangesTest(true);
        if (expectsStaleSchemaDeferral()) {
            assertTrue(logInterceptor.containsMessage("is stale against the database"),
                    "the stale-schema deferral should have been exercised at least once");
        }
    }

    /**
     * Whether {@link #schemaChangesWithColumnFilter()} is expected to exercise the stale-schema
     * deferral. On the binlog family the DDL travels in the log on the same thread as the window
     * watermarks, so the mid-window rotation is structurally guaranteed; connectors where schema
     * changes reach the connector with a lag (for example SQL Server capture instances) typically
     * close the window before the rotation and legitimately never defer.
     */
    protected boolean expectsStaleSchemaDeferral() {
        return true;
    }

    /**
     * @param ddlWhileSnapshotting when {@code true} each group of schema changes is issued from the
     *            record consumer, once the previous batch of snapshot records arrived, so the DDL is
     *            guaranteed to land while the snapshot is in flight rather than before the signal is
     *            processed
     */
    private void executeSchemaChangesTest(boolean ddlWhileSnapshotting) throws Exception {
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignal();

        final List<SchemaChange> schemaChanges = new ArrayList<>();
        schemaChanges.add(new SchemaChange(ROW_COUNT + 10,
                String.format("INSERT INTO %s (pk, aa) VALUES (%s, %s)", tableName(), ROW_COUNT + 11, ROW_COUNT + 10),
                alterColumnStatement(tableName(), "aa", "VARCHAR(5)"),
                String.format("INSERT INTO %s (pk, aa) VALUES (%s, '%s')", tableName(), ROW_COUNT + 10, ROW_COUNT + 9)));
        for (int i = 0; i < 9; i += 3) {
            schemaChanges.add(new SchemaChange(i + ROW_COUNT + 3,
                    String.format("ALTER TABLE %s ADD c INT", tableName()),
                    String.format("INSERT INTO %s (pk, aa, c) VALUES (%s, '%s', %s)", tableName(), i + ROW_COUNT + 1, i + ROW_COUNT, 1),
                    alterColumnStatement(tableName(), "c", "VARCHAR(5)"),
                    String.format("INSERT INTO %s (pk, aa, c) VALUES (%s, '%s', '%s')", tableName(), i + ROW_COUNT + 2, i + ROW_COUNT + 1, "1"),
                    String.format("ALTER TABLE %s DROP COLUMN c", tableName()),
                    String.format("INSERT INTO %s (pk, aa) VALUES (%s, '%s')", tableName(), i + ROW_COUNT + 3, i + ROW_COUNT + 2)));
        }

        final AtomicInteger nextSchemaChange = new AtomicInteger();
        if (!ddlWhileSnapshotting) {
            while (nextSchemaChange.get() < schemaChanges.size()) {
                executeSchemaChange(schemaChanges.get(nextSchemaChange.getAndIncrement()).statements());
            }
        }

        // Each group of statements runs only once the streamed insert that closes the previous group has
        // been received: the DDLs then reach the connector at its own pace, and the snapshot has to cope
        // with a schema change while chunks are in flight rather than with a burst of changes
        final AtomicInteger awaitedPk = new AtomicInteger(-1);
        final int expectedRecordCount = ROW_COUNT + 11;
        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedRecordCount, x -> true, records -> {
            if (!ddlWhileSnapshotting || nextSchemaChange.get() >= schemaChanges.size()) {
                return;
            }
            if (awaitedPk.get() >= 0 && records.stream().noneMatch(record -> isStreamedInsertOf(record, awaitedPk.get()))) {
                return;
            }
            final var schemaChange = schemaChanges.get(nextSchemaChange.getAndIncrement());
            executeSchemaChange(schemaChange.statements());
            awaitedPk.set(schemaChange.lastInsertedPk());
        });
        for (int i = 0; i < expectedRecordCount; i++) {
            assertTrue(dbChanges.containsKey(i + 1), String.format("missing PK %d", i + 1));
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

    private boolean isStreamedInsertOf(SourceRecord record, int pk) {
        final var value = (Struct) record.value();
        return Envelope.Operation.CREATE.code().equals(value.getString(Envelope.FieldName.OPERATION))
                && ((Struct) record.key()).getInt32(pkFieldName()) == pk;
    }

    private record SchemaChange(int lastInsertedPk, String... statements) {
    }

    private void executeSchemaChange(String... statements) {
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            connection.execute(statements);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void renameTable() throws Exception {
        // Testing.Print.enable();

        populateTable();
        final String newTable = "new_table";
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            connection.execute(createTableStatement(tableName(newTable), tableName()));
            connection.execute(String.format("INSERT INTO %s SELECT * FROM %s", tableName(newTable), tableName()));
            connection.execute(String.format("ALTER TABLE %s ADD c varchar(5)", tableName(newTable)));
        }
        startConnector();
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

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
        // Testing.Print.enable();

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
        // Testing.Print.enable();

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
            assertNotNull(value, "value is null at pk=" + (i + 1));
            assertEquals(-6, value, String.format("value is %d at pk = %d, expected -6", value, i + 1));
        }

        for (int i = 2 * ROW_COUNT; i < 3 * ROW_COUNT; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final Struct after = ((Struct) record.value()).getStruct("after");
            final Integer value = after.getInt32(valueFieldName());
            assertNotNull(value, "value is null at pk=" + (i + 1));
            assertEquals(-9, value, String.format("value is %d at pk = %d, expected -9", value, i + 1));
        }

        for (int i = 3 * ROW_COUNT; i < 4 * ROW_COUNT; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final Struct after = ((Struct) record.value()).getStruct("after");
            final Integer value = after.getInt32(valueFieldName());
            assertNull(value, "value is not null at pk=" + (i + 1));
        }
    }

}
