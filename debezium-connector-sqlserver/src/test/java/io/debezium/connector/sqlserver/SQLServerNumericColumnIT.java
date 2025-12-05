/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.util.Testing;

/**
 * Tests for numeric/decimal columsn with precise, string and decimal options
 *
 * @author Pradeep Mamillapalli
 *
 */
public class SQLServerNumericColumnIT extends AbstractAsyncEngineConnectorTest {
    private SqlServerConnection connection;

    /**
     * Create 2 Tables. Each table has 4 columns cola: Decimal(8,4) type with 8
     * precision and 4 scale colb: Decimal - Default precision(18) and default
     * scale(0) colc: numeric(7,1) - 7 precision and 1 scale cold: numeric-
     * Default precision(18) and default scale(0)
     *
     * @throws SQLException
     */
    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablenuma (id int IDENTITY(1,1) primary key, cola DECIMAL(8, 4),colb DECIMAL, colc numeric(8,1), cold numeric)",
                "CREATE TABLE tablenumb (id int IDENTITY(1,1) primary key, cola DECIMAL(8, 4),colb DECIMAL, colc numeric(8,1), cold numeric)",
                "CREATE TABLE tablenumc (id int IDENTITY(1,1) primary key, cola DECIMAL(8, 4),colb DECIMAL, colc numeric(8,1), cold numeric)",
                "CREATE TABLE tablenumd (id int IDENTITY(1,1) primary key, cola DECIMAL(8, 4),colb DECIMAL, colc numeric(8,1), cold numeric)");
        TestHelper.enableTableCdc(connection, "tablenuma");
        TestHelper.enableTableCdc(connection, "tablenumb");
        TestHelper.enableTableCdc(connection, "tablenumc");
        TestHelper.enableTableCdc(connection, "tablenumd");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * Insert 1 Record into tablenuma with {@code DecimalHandlingMode.STRING}
     * mode Assertions: - Connector is running - 1 Record are streamed out of
     * cdc - Assert cola, colb, colc, cold are exactly equal to the input
     * values.
     *
     * @throws Exception
     */
    @Test
    public void decimalModeConfigString() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.tablenuma")
                .with(SqlServerConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING).build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO tablenuma VALUES (111.1111, 1111111, 1111111.1, 1111111 );");
        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.testDB1.dbo.tablenuma");
        assertThat(tableA).hasSize(1);
        final Struct valueA = (Struct) tableA.get(0).value();
        assertSchema(valueA, Schema.OPTIONAL_STRING_SCHEMA);
        assertThat(((Struct) valueA.get("after")).get("cola")).isEqualTo("111.1111");
        assertThat(((Struct) valueA.get("after")).get("colb")).isEqualTo("1111111");
        assertThat(((Struct) valueA.get("after")).get("colc")).isEqualTo("1111111.1");
        assertThat(((Struct) valueA.get("after")).get("cold")).isEqualTo("1111111");
        stopConnector();
    }

    /**
     * Insert 1 Record into tablenumb with {@code DecimalHandlingMode.DOUBLE}
     * mode Assertions: - Connector is running - 1 Record are streamed out of
     * cdc - Assert cola, colb, colc, cold are exactly equal to the input values
     * in double format
     *
     * @throws Exception
     */
    @Test
    public void decimalModeConfigDouble() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.tablenumb")
                .with(SqlServerConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE).build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO tablenumb VALUES (222.2222, 22222, 22222.2, 2222222 );");
        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> results = records.recordsForTopic("server1.testDB1.dbo.tablenumb");
        assertThat(results).hasSize(1);
        final Struct valueA = (Struct) results.get(0).value();
        assertSchema(valueA, Schema.OPTIONAL_FLOAT64_SCHEMA);
        assertThat(((Struct) valueA.get("after")).get("cola")).isEqualTo(222.2222d);
        assertThat(((Struct) valueA.get("after")).get("colb")).isEqualTo(22222d);
        assertThat(((Struct) valueA.get("after")).get("colc")).isEqualTo(22222.2d);
        assertThat(((Struct) valueA.get("after")).get("cold")).isEqualTo(2222222d);
        stopConnector();
    }

    /**
     * Insert 1 Record into tablenumc with {@code DecimalHandlingMode.PRECISE}
     * mode Assertions: - Connector is running - 1 Record are streamed out of
     * cdc - Assert cola, colb, colc, cold are bytes
     *
     * @throws Exception
     */
    @Test
    public void decimalModeConfigPrecise() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.tablenumc")
                .with(SqlServerConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE).build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();

        connection.execute("INSERT INTO tablenumc VALUES (333.3333, 3333, 3333.3, 33333333 );");
        final SourceRecords records = consumeRecordsByTopic(1);
        final List<SourceRecord> results = records.recordsForTopic("server1.testDB1.dbo.tablenumc");
        assertThat(results).hasSize(1);
        final Struct valueA = (Struct) results.get(0).value();
        assertThat(valueA.schema().field("after").schema().field("cola").schema())
                .isEqualTo(Decimal.builder(4).parameter("connect.decimal.precision", "8").optional().schema());
        assertThat(valueA.schema().field("after").schema().field("colb").schema())
                .isEqualTo(Decimal.builder(0).parameter("connect.decimal.precision", "18").optional().schema());
        assertThat(valueA.schema().field("after").schema().field("colc").schema())
                .isEqualTo(Decimal.builder(1).parameter("connect.decimal.precision", "8").optional().schema());
        assertThat(valueA.schema().field("after").schema().field("cold").schema())
                .isEqualTo(Decimal.builder(0).parameter("connect.decimal.precision", "18").optional().schema());
        assertThat(((Struct) valueA.get("after")).get("cola")).isEqualTo(BigDecimal.valueOf(333.3333));
        assertThat(((Struct) valueA.get("after")).get("colb")).isEqualTo(BigDecimal.valueOf(3333));
        assertThat(((Struct) valueA.get("after")).get("colc")).isEqualTo(BigDecimal.valueOf(3333.3));
        assertThat(((Struct) valueA.get("after")).get("cold")).isEqualTo(BigDecimal.valueOf(33333333));
        stopConnector();
    }

    private void assertSchema(Struct valueA, Schema expected) {
        assertThat(valueA.schema().field("after").schema().field("cola").schema()).isEqualTo(expected);
        assertThat(valueA.schema().field("after").schema().field("colb").schema()).isEqualTo(expected);
        assertThat(valueA.schema().field("after").schema().field("colc").schema()).isEqualTo(expected);
        assertThat(valueA.schema().field("after").schema().field("cold").schema()).isEqualTo(expected);
    }
}
