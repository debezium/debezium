/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test for using snapshot SELECT overrides with the Debezium Oracle connector.
 *
 * @author Chris Cranford
 */
public class SnapshotSelectOverridesIT extends AbstractConnectorTest {

    private static final int INITIAL_RECORDS_PER_TABLE = 10;

    private static final String DDL = "CREATE TABLE %s (ID numeric(9,0) primary key, NAME varchar2(30), " +
            "PRICE numeric(8,2), TS date, SOFT_DELETED number(1))";

    private static final String DML = "INSERT INTO %s VALUES (%s, '%s', %s, TO_DATE('%s','yyyy-mm-dd'), %s)";

    private OracleConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        cleanupTables();

        // create tables
        connection.execute(String.format(DDL, "table1"));
        connection.execute(String.format(DDL, "table2"));
        connection.execute(String.format(DDL, "table3"));

        // Populate database
        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE; i++) {
            connection.execute(String.format(DML, "table1", i, "name" + i, new BigDecimal(i + ".23"), "2018-07-18", i % 2));
            connection.execute(String.format(DML, "table2", i, "name" + i, new BigDecimal(i + ".23"), "2018-07-18", i % 2));
            connection.execute(String.format(DML, "table3", i, "name" + i, new BigDecimal(i + ".23"), "2018-07-18", i % 2));
        }

        // setup tables for streaming
        TestHelper.streamTable(connection, "debezium.table1");
        TestHelper.streamTable(connection, "debezium.table2");
        TestHelper.streamTable(connection, "debezium.table3");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            cleanupTables();
            connection.close();
        }
    }

    private void cleanupTables() {
        TestHelper.dropTable(connection, "debezium.table1");
        TestHelper.dropTable(connection, "debezium.table2");
        TestHelper.dropTable(connection, "debezium.table3");
    }

    @Test
    @FixFor("DBZ-3250")
    public void takeSnapshotWithOverrides() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(TABLE_INCLUDE_LIST, "DEBEZIUM\\.TABLE[1-3]")
                .with(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "DEBEZIUM.TABLE1,DEBEZIUM.TABLE3")
                .with(
                        SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + ".DEBEZIUM.TABLE1",
                        "SELECT * FROM DEBEZIUM.TABLE1 WHERE SOFT_DELETED = 0 ORDER BY ID DESC")
                .with(
                        SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + ".DEBEZIUM.TABLE3",
                        "SELECT * FROM DEBEZIUM.TABLE3 WHERE SOFT_DELETED = 0")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        int expected = INITIAL_RECORDS_PER_TABLE + (INITIAL_RECORDS_PER_TABLE + INITIAL_RECORDS_PER_TABLE) / 2;
        SourceRecords records = consumeRecordsByTopic(expected);
        List<SourceRecord> table1 = records.recordsForTopic("server1.DEBEZIUM.TABLE1");
        List<SourceRecord> table2 = records.recordsForTopic("server1.DEBEZIUM.TABLE2");
        List<SourceRecord> table3 = records.recordsForTopic("server1.DEBEZIUM.TABLE3");

        // soft_deleted records should be excluded for table1 and table3
        assertThat(table1).hasSize(INITIAL_RECORDS_PER_TABLE / 2);
        assertThat(table2).hasSize(INITIAL_RECORDS_PER_TABLE);
        assertThat(table3).hasSize(INITIAL_RECORDS_PER_TABLE / 2);

        StringBuilder actualIdsForTable1 = new StringBuilder();
        for (int i = 0; i < INITIAL_RECORDS_PER_TABLE / 2; ++i) {
            SourceRecord record = table1.get(i);

            Struct key = (Struct) record.key();
            actualIdsForTable1.append(key.get("ID"));

            // soft_deleted records should be excluded
            Struct value = (Struct) record.value();
            assertThat(((Struct) value.get(Envelope.FieldName.AFTER)).get("SOFT_DELETED")).isEqualTo((byte) 0);
        }

        // the ORDER BY clause should be applied, too
        final String expectedIdsForTable1 = "86420";
        assertThat(actualIdsForTable1.toString()).isEqualTo(expectedIdsForTable1);
    }
}
