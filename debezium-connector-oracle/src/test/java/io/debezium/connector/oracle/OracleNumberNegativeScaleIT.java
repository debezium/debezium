/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Tests Oracle NUMBER type with negative scale.
 *
 * @author vjuranek
 */
public class OracleNumberNegativeScaleIT extends AbstractConnectorTest {

    private static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";
    private static final Schema NUMBER_SCHEMA = Decimal.builder(0).optional().parameter(PRECISION_PARAMETER_KEY, "38").build();

    private static final String TEST_TABLE_DDL = "create table debezium.number_negative_scale (" +
            "  id numeric(9,0) not null, " +
            "  val_number_2_negative_scale number(1, -1), " +
            "  val_number_4_negative_scale number(2, -2), " +
            "  val_number_9_negative_scale number(8, -1), " +
            "  val_number_18_negative_scale number(16, -2), " +
            "  val_number_36_negative_scale number(36, -2), " +
            "  primary key (id)" +
            ")";

    private static SchemaAndValueField[] expectedRecords = {
            new SchemaAndValueField("VAL_NUMBER_2_NEGATIVE_SCALE", Schema.OPTIONAL_INT8_SCHEMA, (byte) 90),
            new SchemaAndValueField("VAL_NUMBER_4_NEGATIVE_SCALE", Schema.OPTIONAL_INT16_SCHEMA, (short) 9900),
            new SchemaAndValueField("VAL_NUMBER_9_NEGATIVE_SCALE", Schema.OPTIONAL_INT32_SCHEMA, 9999_99990),
            new SchemaAndValueField("VAL_NUMBER_18_NEGATIVE_SCALE", Schema.OPTIONAL_INT64_SCHEMA, 999_99999_99999_99900L),
            new SchemaAndValueField("VAL_NUMBER_36_NEGATIVE_SCALE", Decimal.builder(0).optional()
                    .parameter(PRECISION_PARAMETER_KEY, "36").build(),
                    new BigDecimal(
                            new BigInteger("99999999999999999999999999999999999900"), 0)) };

    private OracleConnection connection;

    @Before
    public void before() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "debezium.number_negative_scale");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        connection.execute(TEST_TABLE_DDL);
        TestHelper.streamTable(connection, "debezium.number_negative_scale");
    }

    @After
    public void after() throws Exception {
        if (connection != null && connection.isConnected()) {
            TestHelper.dropTable(connection, "debezium.number_negative_scale");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-5880")
    public void testNegativeScaleWithPrecisionHandling() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.number_negative_scale")
                .with(OracleConnectorConfig.CUSTOM_CONVERTERS, "zero_scale")
                .with("zero_scale.type", "io.debezium.connector.oracle.converters.NumberToZeroScaleConverter")
                .build();

        insertAndStreamNegativeScaleNumbers(config);
    }

    @Test
    @FixFor("DBZ-5880")
    public void testNegativeScaleWithStringHandling() throws Exception {
        expectedRecords[4] = new SchemaAndValueField("VAL_NUMBER_36_NEGATIVE_SCALE", SchemaBuilder.string().optional().build(), "99999999999999999999999999999999999900");

        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.number_negative_scale")
                .with(OracleConnectorConfig.CUSTOM_CONVERTERS, "zero_scale")
                .with("zero_scale.type", "io.debezium.connector.oracle.converters.NumberToZeroScaleConverter")
                .with("zero_scale.decimal.mode", "string")
                .build();

        insertAndStreamNegativeScaleNumbers(config);
    }

    @Test
    @FixFor("DBZ-5880")
    public void testNegativeScaleWithDoubleHandling() throws Exception {
        expectedRecords[4] = new SchemaAndValueField("VAL_NUMBER_36_NEGATIVE_SCALE", SchemaBuilder.float64().optional().build(),
                99999999999999999999999999999999999900.0);

        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.number_negative_scale")
                .with(OracleConnectorConfig.CUSTOM_CONVERTERS, "zero_scale")
                .with("zero_scale.type", "io.debezium.connector.oracle.converters.NumberToZeroScaleConverter")
                .with("zero_scale.decimal.mode", "double")
                .build();

        insertAndStreamNegativeScaleNumbers(config);
    }

    protected void insertAndStreamNegativeScaleNumbers(Configuration config) throws Exception {
        insertIntTypes();

        initializeConnectorTestFramework();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.NUMBER_NEGATIVE_SCALE");

        assertThat(testTableRecords).hasSize(1);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);
        VerifyRecord.isValidRead(record, "ID", 1);

        Struct after = (Struct) ((Struct) record.value()).get("after");
        Arrays.asList(expectedRecords).forEach(schemaAndValueField -> schemaAndValueField.assertFor(after));
        stopConnector();
    }

    protected void insertIntTypes() throws SQLException {
        connection.execute(
                "INSERT INTO debezium.number_negative_scale VALUES (1, 94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949)");
        connection.execute("COMMIT");
    }

}
