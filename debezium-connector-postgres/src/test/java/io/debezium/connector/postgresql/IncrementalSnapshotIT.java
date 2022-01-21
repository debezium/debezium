/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.fest.assertions.Assertions;
import org.fest.assertions.MapAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotTest<PostgresConnector> {

    private static final String TOPIC_NAME = "test_server.s1.a";

    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" + "CREATE SCHEMA s1; "
            + "CREATE SCHEMA s2; " + "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));"
            + "CREATE TABLE s1.a4 (pk1 integer, pk2 integer, pk3 integer, pk4 integer, aa integer, PRIMARY KEY(pk1, pk2, pk3, pk4));"
            + "CREATE TABLE s1.a42 (pk1 integer, pk2 integer, pk3 integer, pk4 integer, aa integer);"
            + "CREATE TABLE s1.anumeric (pk numeric, aa integer, PRIMARY KEY(pk));"
            + "CREATE TABLE s1.debezium_signal (id varchar(64), type varchar(32), data varchar(2048));";

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();
        initializeConnectorTestFramework();

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SIGNAL_DATA_COLLECTION, "s1.debezium_signal")
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "s1.a42:pk1,pk2,pk3,pk4")
                // DBZ-4272 required to allow dropping columns just before an incremental snapshot
                .with("database.autosave", "conservative");
    }

    @Override
    protected Class<PostgresConnector> connectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return TestHelper.create();
    }

    @Override
    protected String topicName() {
        return TOPIC_NAME;
    }

    @Override
    protected String tableName() {
        return "s1.a";
    }

    @Override
    protected String signalTableName() {
        return "s1.debezium_signal";
    }

    @Override
    protected void waitForConnectorToStart() {
        super.waitForConnectorToStart();
        TestHelper.waitForDefaultReplicationSlotBeActive();
    }

    @Test
    public void inserts4Pks() throws Exception {
        // Testing.Print.enable();

        populate4PkTable();
        startConnector();

        sendAdHocSnapshotSignal("s1.a4");

        Thread.sleep(5000);
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                final int id = i + ROW_COUNT + 1;
                final int pk1 = id / 1000;
                final int pk2 = (id / 100) % 10;
                final int pk3 = (id / 10) % 10;
                final int pk4 = id % 10;
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (pk1, pk2, pk3, pk4, aa) VALUES (%s, %s, %s, %s, %s)",
                        "s1.a4",
                        pk1,
                        pk2,
                        pk3,
                        pk4,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32("pk1") * 1_000 + k.getInt32("pk2") * 100 + k.getInt32("pk3") * 10 + k.getInt32("pk4"),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                "test_server.s1.a4",
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void insertsWithoutPks() throws Exception {
        // Testing.Print.enable();

        populate4WithoutPkTable();
        startConnector();

        sendAdHocSnapshotSignal("s1.a42");

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32("pk1") * 1_000 + k.getInt32("pk2") * 100 + k.getInt32("pk3") * 10 + k.getInt32("pk4"),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                "test_server.s1.a42",
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void insertsNumericPk() throws Exception {
        // Testing.Print.enable();

        try (final JdbcConnection connection = databaseConnection()) {
            populateTable(connection, "s1.anumeric");
        }
        startConnector();

        sendAdHocSnapshotSignal("s1.anumeric");

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> VariableScaleDecimal.toLogical(k.getStruct("pk")).getWrappedValue().intValue(),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                "test_server.s1.anumeric",
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    protected void populate4PkTable() throws SQLException {
        try (final JdbcConnection connection = databaseConnection()) {
            populate4PkTable(connection, "s1.a4");
        }
    }

    protected void populate4WithoutPkTable() throws SQLException {
        try (final JdbcConnection connection = databaseConnection()) {
            populate4PkTable(connection, "s1.a42");
        }
    }
}
