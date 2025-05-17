/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

/**
 * @author Omar Al-Safi
 */
public abstract class BinlogUnsignedIntegerIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {
    private static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";
    private static final Schema BIGINT_PRECISE_SCHEMA = Decimal.builder(0).parameter(PRECISION_PARAMETER_KEY, "20").build();

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-json.txt")
            .toAbsolutePath();

    private Configuration config;

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("unsignednumericit", "unsigned_integer_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }

        dropAllDatabases();
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .with(BinlogConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE, BinlogConnectorConfig.BigIntUnsignedHandlingMode.PRECISE)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 7;
        int numDataRecords = numCreateTables * 3; // Total data records
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("unsignednumericit").size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_tinyint_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_smallint_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_mediumint_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_int_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_bigint_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(
                numCreateDatabase + numCreateTables);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("json_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("geometry_test")).isNull();
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_228_int_unsigned")) {
                assertIntUnsigned(value);
            }
            else if (record.topic().endsWith("dbz_228_tinyint_unsigned")) {
                assertTinyintUnsigned(value);
            }
            else if (record.topic().endsWith("dbz_228_smallint_unsigned")) {
                assertSmallUnsigned(value);
            }
            else if (record.topic().endsWith("dbz_228_mediumint_unsigned")) {
                assertMediumUnsigned(value);
            }
            else if (record.topic().endsWith("dbz_228_bigint_unsigned")) {
                assertBigintUnsignedPrecise(value);
            }
        });
        assertSerialPrecise(records.recordsForTopic(DATABASE.topicForTable("dbz_1185_serial")));
    }

    @Test
    @FixFor("DBZ-363")
    public void shouldConsumeAllEventsFromBigIntTableInDatabaseUsingBinlogAndNoSnapshotUsingLong() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER.toString())
                .with(BinlogConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE, BinlogConnectorConfig.BigIntUnsignedHandlingMode.LONG)
                .build();
        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 7;
        int numDataRecords = numCreateTables * 3; // Total data records
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_bigint_unsigned")).size()).isEqualTo(3);
        assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_228_bigint_unsigned")) {
                assertBigintUnsignedLong(value);
            }
        });
        assertSerial(records.recordsForTopic(DATABASE.topicForTable("dbz_1185_serial")));
        assertSerialDefaultValue(records.recordsForTopic(DATABASE.topicForTable("dbz_1185_serial_default_value")));
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig().build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numTables = 7;
        int numDataRecords = numTables * 3;
        int numDdlRecords = numTables * 2 + 3; // for each table (1 drop + 1 create) + for each db (1 create + 1 drop + 1 use)
        int numSetVariables = 1;
        SourceRecords records = consumeRecordsByTopic(numDdlRecords + numSetVariables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("unsignednumericit").size()).isEqualTo(numDdlRecords + numSetVariables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_tinyint_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_smallint_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_mediumint_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_int_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_228_bigint_unsigned")).size())
                .isEqualTo(3);
        assertThat(records.topics().size()).isEqualTo(numTables + 1);
        assertThat(records.databaseNames()).containsOnly(DATABASE.getDatabaseName(), "");
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(numDdlRecords);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("json_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("geometry_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1); // SET statement
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_228_int_unsigned")) {
                assertIntUnsigned(value);
            }
            else if (record.topic().endsWith("dbz_228_tinyint_unsigned")) {
                assertTinyintUnsigned(value);
            }
            else if (record.topic().endsWith("dbz_228_smallint_unsigned")) {
                assertSmallUnsigned(value);
            }
            else if (record.topic().endsWith("dbz_228_mediumint_unsigned")) {
                assertMediumUnsigned(value);
            }
            else if (record.topic().endsWith("dbz_228_bigint_unsigned")) {
                assertBigintUnsignedLong(value);
            }
        });
        assertSerial(records.recordsForTopic(DATABASE.topicForTable("dbz_1185_serial")));
        assertSerialDefaultValue(records.recordsForTopic(DATABASE.topicForTable("dbz_1185_serial_default_value")));
    }

    private void assertTinyintUnsigned(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        // Validate the schema first, we are expecting int-16 since we are dealing with unsignd-tinyint
        // So Unsigned TINYINT would be an INT16 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(Schema.INT16_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(Schema.INT16_SCHEMA);

        // Validate the schema first, we are expecting int-16 since we are dealing with signed-tinyint.
        // Note: the recommended mapping of Signed TINYINT is Short which is 16-bit. http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
        // So Signed TINYINT would be an INT16 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT16_SCHEMA);

        // Validate candidates values
        switch (i) {
            case 1:
                assertThat(after.getInt16("c1")).isEqualTo((short) 255);
                assertThat(after.getInt16("c2")).isEqualTo((short) (255));
                assertThat(after.getInt16("c3")).isEqualTo((short) 127);
                break;
            case 2:
                assertThat(after.getInt16("c1")).isEqualTo((short) 155);
                assertThat(after.getInt16("c2")).isEqualTo((short) 155);
                assertThat(after.getInt16("c3")).isEqualTo((short) -100);
                break;
            case 3:
                assertThat(after.getInt16("c1")).isEqualTo((short) 0);
                assertThat(after.getInt16("c2")).isEqualTo((short) 0);
                assertThat(after.getInt16("c3")).isEqualTo((short) -128);
        }
    }

    private void assertSmallUnsigned(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        // Validate the schema first, we are expecting int-32 since we are dealing with unsignd-smallint
        // So Unsigned SMALLINT would be an int32 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(Schema.INT32_SCHEMA);

        // Validate the schema first, we are expecting int-16 since we are dealing with signed-smallint.
        // So Signed SMALLINT would be an INT16 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT16_SCHEMA);

        // Validate candidates values
        switch (i) {
            case 1:
                assertThat(after.getInt32("c1")).isEqualTo(65535);
                assertThat(after.getInt32("c2")).isEqualTo(65535);
                assertThat(after.getInt16("c3")).isEqualTo((short) 32767);
                break;
            case 2:
                assertThat(after.getInt32("c1")).isEqualTo(45535);
                assertThat(after.getInt32("c2")).isEqualTo(45535);
                assertThat(after.getInt16("c3")).isEqualTo((short) -12767);
                break;
            case 3:
                assertThat(after.getInt32("c1")).isEqualTo(0);
                assertThat(after.getInt32("c2")).isEqualTo(0);
                assertThat(after.getInt16("c3")).isEqualTo((short) -32768);
        }
    }

    private void assertMediumUnsigned(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        // Validate the schema first, we are expecting int-32 since we are dealing with unsignd-mediumint
        // So Unsigned MEDIUMINT would be an int32 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(Schema.INT32_SCHEMA);

        // Validate the schema first, we are expecting int-32 since we are dealing with signed-mediumint.
        // So Signed MEDIUMINT would be an INT32 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT32_SCHEMA);

        // Validate candidates values
        switch (i) {
            case 1:
                assertThat(after.getInt32("c1")).isEqualTo(16777215);
                assertThat(after.getInt32("c2")).isEqualTo(16777215);
                assertThat(after.getInt32("c3")).isEqualTo(8388607);
                break;
            case 2:
                assertThat(after.getInt32("c1")).isEqualTo(10777215);
                assertThat(after.getInt32("c2")).isEqualTo(10777215);
                assertThat(after.getInt32("c3")).isEqualTo(-6388607);
                break;
            case 3:
                assertThat(after.getInt32("c1")).isEqualTo(0);
                assertThat(after.getInt32("c2")).isEqualTo(0);
                assertThat(after.getInt32("c3")).isEqualTo(-8388608);
        }
    }

    private void assertIntUnsigned(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        // Validate the schema first, we are expecting int-64 since we are dealing with unsignd-int
        // So Unsigned INT would be an INT64 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(after.schema().field("c4").schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(after.schema().field("c5").schema()).isEqualTo(Schema.INT64_SCHEMA);

        // Validate the schema first, we are expecting int-32 since we are dealing with signed-int
        // So Signed INT would be an INT32 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(after.schema().field("c6").schema()).isEqualTo(Schema.INT32_SCHEMA);

        // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html

        // Validate candidates values
        switch (i) {
            case 1:
                assertThat(after.getInt64("c1")).isEqualTo(4294967295L);
                assertThat(after.getInt64("c2")).isEqualTo(4294967295L);
                assertThat(after.getInt32("c3")).isEqualTo(2147483647);
                assertThat(after.getInt64("c4")).isEqualTo(4294967295L);
                assertThat(after.getInt64("c5")).isEqualTo(4294967295L);
                assertThat(after.getInt32("c6")).isEqualTo(2147483647);
                break;
            case 2:
                assertThat(after.getInt64("c1")).isEqualTo(3294967295L);
                assertThat(after.getInt64("c2")).isEqualTo(3294967295L);
                assertThat(after.getInt32("c3")).isEqualTo(-1147483647);
                assertThat(after.getInt64("c4")).isEqualTo(3294967295L);
                assertThat(after.getInt64("c5")).isEqualTo(3294967295L);
                assertThat(after.getInt32("c6")).isEqualTo(-1147483647);
                break;
            case 3:
                assertThat(after.getInt64("c1")).isEqualTo(0L);
                assertThat(after.getInt64("c2")).isEqualTo(0L);
                assertThat(after.getInt32("c3")).isEqualTo(-2147483648);
                assertThat(after.getInt64("c4")).isEqualTo(0L);
                assertThat(after.getInt64("c5")).isEqualTo(0L);
                assertThat(after.getInt32("c6")).isEqualTo(-2147483648);
        }
    }

    private void assertBigintUnsignedPrecise(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        // Validate the schema first, we are expecting org.apache.kafka.connect.data.Decimal:Byte since we are dealing with unsignd-bigint
        // So Unsigned BIGINT would be an int32 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(BIGINT_PRECISE_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(BIGINT_PRECISE_SCHEMA);

        // Validate the schema first, we are expecting int-64 since we are dealing with signed-bigint.
        // So Signed BIGINT would be an INT64 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT64_SCHEMA);

        // Validate candidates values
        switch (i) {
            case 1:
                assertThat(after.get("c1")).isEqualTo(new BigDecimal("18446744073709551615"));
                assertThat(after.get("c2")).isEqualTo(new BigDecimal("18446744073709551615"));
                assertThat(after.getInt64("c3")).isEqualTo(9223372036854775807L);
                break;
            case 2:
                assertThat(after.get("c1")).isEqualTo(new BigDecimal("14446744073709551615"));
                assertThat(after.get("c2")).isEqualTo(new BigDecimal("14446744073709551615"));
                assertThat(after.getInt64("c3")).isEqualTo(-1223372036854775807L);
                break;
            case 3:
                assertThat(after.get("c1")).isEqualTo(new BigDecimal("0"));
                assertThat(after.get("c2")).isEqualTo(new BigDecimal("0"));
                assertThat(after.getInt64("c3")).isEqualTo(-9223372036854775808L);
        }
    }

    private void assertBigintUnsignedLong(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        // Validate the schema first, we are expecting int-64 since we have forced Long mode for BIGINT UNSIGNED
        assertThat(after.schema().field("c1").schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(Schema.INT64_SCHEMA);

        // Validate the schema first, we are expecting int-64 since we are dealing with signed-bigint.
        // So Signed BIGINT would be an INT64 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT64_SCHEMA);

        // Validate candidates values, note the loss in precision which is expected since BIGINT UNSIGNED cannot always be represented by
        // a long datatype.
        switch (i) {
            case 1:
                assertThat(after.getInt64("c1")).isEqualTo(-1L);
                assertThat(after.getInt64("c2")).isEqualTo(-1L);
                assertThat(after.getInt64("c3")).isEqualTo(9223372036854775807L);
                break;
            case 2:
                assertThat(after.getInt64("c1")).isEqualTo(-4000000000000000001L);
                assertThat(after.getInt64("c2")).isEqualTo(-4000000000000000001L);
                assertThat(after.getInt64("c3")).isEqualTo(-1223372036854775807L);
                break;
            case 3:
                assertThat(after.getInt64("c1")).isEqualTo(0L);
                assertThat(after.getInt64("c2")).isEqualTo(0L);
                assertThat(after.getInt64("c3")).isEqualTo(-9223372036854775808L);
        }
    }

    private void assertSerial(List<SourceRecord> records) {
        final long[] expected = new long[]{ 10, 11, -1 };
        assertThat(records).hasSize(3);
        for (int i = 0; i < 3; i++) {
            final Struct after = ((Struct) records.get(i).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.schema().field("id").schema()).isEqualTo(Schema.INT64_SCHEMA);
            final Long id = after.getInt64("id");
            assertThat(id).isNotNull();
            assertThat(id).isEqualTo(expected[i]);
        }
    }

    private void assertSerialPrecise(List<SourceRecord> records) {
        final BigDecimal[] expected = new BigDecimal[]{ new BigDecimal(10), new BigDecimal(11), new BigDecimal("18446744073709551615") };
        assertThat(records).hasSize(3);
        for (int i = 0; i < 3; i++) {
            final Struct after = ((Struct) records.get(i).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.schema().field("id").schema()).isEqualTo(BIGINT_PRECISE_SCHEMA);
            final BigDecimal id = (BigDecimal) after.get("id");
            assertThat(id).isNotNull();
            assertThat(id).isEqualTo(expected[i]);
        }
    }

    private void assertSerialDefaultValue(List<SourceRecord> records) {
        final int[] expected = new int[]{ 10, 11, 1000 };
        assertThat(records).hasSize(3);
        for (int i = 0; i < 3; i++) {
            final Struct after = ((Struct) records.get(i).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.schema().field("id").schema()).isEqualTo(Schema.INT32_SCHEMA);
            final Integer id = after.getInt32("id");
            assertThat(id).isNotNull();
            assertThat(id).isEqualTo(expected[i]);
        }
    }
}
