/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Clob;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Integration tests when LOB is enabled and the primary key changes, to re-select LOB columns
 * within the {@link BaseChangeRecordEmitter}.
 *
 * @author Chris Cranford
 */
public class OraclePrimaryKeyLobReselectIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private OracleConnection connection;

    @Before
    public void beforeEach() {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() throws Exception {
        super.stopConnector();

        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-7458")
    public void testCharColumn() throws Exception {
        testPrimaryKeyChangeReselect("char(5)", "'no'");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testNationalizedCharColumn() throws Exception {
        testPrimaryKeyChangeReselect("nchar(5)", "'no'");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testVarchar2Column() throws Exception {
        testPrimaryKeyChangeReselect("varchar2(50)", "'insert'");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testNationalizedVarchar2Column() throws Exception {
        testPrimaryKeyChangeReselect("nvarchar2(50)", "'insert'");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testNumericColumnNotVariableScaleDecimal() throws Exception {
        testPrimaryKeyChangeReselect("numeric(18,0)", "25");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testNumeric38Column() throws Exception {
        testPrimaryKeyChangeReselect("numeric(38,0)", "25");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testFloatColumn() throws Exception {
        testPrimaryKeyChangeReselect("float(38)", "25");
    }

    @Test
    @FixFor("DBZ-7458")
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.OLR, reason = "BINARY_FLOAT not supported")
    public void testBinaryFloatColumn() throws Exception {
        testPrimaryKeyChangeReselect("binary_float", "25");
    }

    @Test
    @FixFor("DBZ-7458")
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.OLR, reason = "BINARY_DOUBLE not supported")
    public void testBinaryDoubleColumn() throws Exception {
        testPrimaryKeyChangeReselect("binary_double", "25");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testDateColumn() throws Exception {
        testPrimaryKeyChangeReselect("date", "sysdate");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testTimestampColumn() throws Exception {
        testPrimaryKeyChangeReselect("timestamp", "current_timestamp");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testIntervalYearToMonthColumn() throws Exception {
        testPrimaryKeyChangeReselect("interval year to month", "INTERVAL '-3-6' YEAR TO MONTH");
    }

    @Test
    @FixFor("DBZ-7458")
    public void testIntervalDayToSecondColumn() throws Exception {
        testPrimaryKeyChangeReselect("interval day(3) to second(2)", "INTERVAL '-1 2:3:4.56' DAY TO SECOND");
    }

    /**
     * Types the primary-key change reselection process with a specific key column type and inserted value.
     *
     * Internally the method uses a composite key and the numeric {@code id} field is what is mutated, which
     * triggers the LOB-based emitter reselection.
     *
     * @param keyType the key column type
     * @param keyValue the key value to insert
     */
    private void testPrimaryKeyChangeReselect(String keyType, String keyValue) throws Exception {
        TestHelper.dropTable(connection, "dbz7458");
        try {
            connection.execute(String.format(
                    "CREATE TABLE dbz7458 (id numeric(9,0), other_id %s, data clob, primary key(id, other_id))",
                    keyType));

            TestHelper.streamTable(connection, "dbz7458");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.DBZ7458")
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert test row
            final String data = RandomStringUtils.randomAlphanumeric(16384);
            final Clob clob = connection.connection().createClob();
            clob.setString(1, data);
            connection.prepareQuery(String.format("INSERT INTO dbz7458 (id,other_id,data) values (1,%s,?)", keyValue),
                    ps -> ps.setClob(1, clob),
                    null);
            connection.commit();

            // Update row without changing LOB
            connection.execute("UPDATE dbz7458 SET id = 2 WHERE id = 1");

            // There will be the original insert, the delete, tombstone, and the insert
            // The last three are based on the primary-key update logic in BaseChangeRecordEmitter
            final SourceRecords sourceRecords = consumeRecordsByTopic(4);

            final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ7458");
            assertThat(tableRecords).hasSize(4);

            // Fetch the record that will contain the reselected values for the insert
            final SourceRecord insert = tableRecords.get(3);
            VerifyRecord.isValidInsert(insert, "ID", 2);

            // Verify that the LOB column was re-selected
            final Struct after = ((Struct) insert.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("DATA")).isEqualTo(data);
        }
        finally {
            TestHelper.dropTable(connection, "dbz7458");
        }
    }
}
