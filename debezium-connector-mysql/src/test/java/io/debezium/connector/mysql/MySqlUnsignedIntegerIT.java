/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.SQLException;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Omar Al-Safi
 */
public class MySqlUnsignedIntegerIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-json.txt")
            .toAbsolutePath();

    private Configuration config;

    private static final String SERVER_NAME = "unsignednumericit";
    private static final String DATABASE_NAME = "unsigned_integer_test";

    @Before
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                .with(MySqlConnectorConfig.USER, "snapper")
                .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.SecureConnectionMode.DISABLED)
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.SERVER_NAME, SERVER_NAME)
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.DATABASE_WHITELIST, DATABASE_NAME)
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 5;
        int numDataRecords = numCreateTables * 3; //Total data records
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(SERVER_NAME).size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_tinyint_unsigned").size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_smallint_unsigned").size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_mediumint_unsigned").size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_int_unsigned").size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_bigint_unsigned").size())
                .isEqualTo(3);
        assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE_NAME).size()).isEqualTo(
                numCreateDatabase + numCreateTables);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("json_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("geometry_test")).isNull();
        records.ddlRecordsForDatabase(DATABASE_NAME).forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_228_int_unsigned")) {
                assertIntUnsigned(value);
            } else if (record.topic().endsWith("dbz_228_tinyint_unsigned")) {
                assertTinyintUnsigned(value);
            } else if (record.topic().endsWith("dbz_228_smallint_unsigned")) {
                assertSmallUnsigned(value);
            } else if (record.topic().endsWith("dbz_228_mediumint_unsigned")) {
                assertMediumUnsigned(value);
            } else if (record.topic().endsWith("dbz_228_bigint_unsigned")) {
                assertBigintUnsigned(value);
            }
        });
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                .with(MySqlConnectorConfig.USER, "snapper")
                .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.SecureConnectionMode.DISABLED)
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.SERVER_NAME, SERVER_NAME)
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.DATABASE_WHITELIST, DATABASE_NAME)
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        //Testing.Debug.enable();
        int numTables = 5;
        int numDataRecords = numTables * 3;
        int numDdlRecords =
                numTables * 2 + 3; // for each table (1 drop + 1 create) + for each db (1 create + 1 drop + 1 use)
        int numSetVariables = 1;
        SourceRecords records = consumeRecordsByTopic(numDdlRecords + numSetVariables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(SERVER_NAME).size()).isEqualTo(numDdlRecords + numSetVariables);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_tinyint_unsigned").size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_smallint_unsigned").size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_mediumint_unsigned").size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_int_unsigned").size())
                .isEqualTo(3);
        assertThat(records.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".dbz_228_bigint_unsigned").size())
                .isEqualTo(3);
        assertThat(records.topics().size()).isEqualTo(numTables + 1);
        assertThat(records.databaseNames()).containsOnly(DATABASE_NAME, "");
        assertThat(records.ddlRecordsForDatabase(DATABASE_NAME).size()).isEqualTo(numDdlRecords);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("json_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("geometry_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1); // SET statement
        records.ddlRecordsForDatabase(DATABASE_NAME).forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_228_int_unsigned")) {
                assertIntUnsigned(value);
            } else if (record.topic().endsWith("dbz_228_tinyint_unsigned")) {
                assertTinyintUnsigned(value);
            } else if (record.topic().endsWith("dbz_228_smallint_unsigned")) {
                assertSmallUnsigned(value);
            } else if (record.topic().endsWith("dbz_228_mediumint_unsigned")) {
                assertMediumUnsigned(value);
            } else if (record.topic().endsWith("dbz_228_bigint_unsigned")) {
                assertBigintUnsigned(value);
            }
        });
    }

    private void assertTinyintUnsigned(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        //Validate the schema first, we are expecting int-16 since we are dealing with unsignd-tinyint
        //So Unsigned TINYINT would be an INT16 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(Schema.INT16_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(Schema.INT16_SCHEMA);

        //Validate the schema first, we are expecting int-16 since we are dealing with signed-tinyint.
        // Note: the recommended mapping of Signed TINYINT is Short which is 16-bit. http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
        //So Signed TINYINT would be an INT16 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT16_SCHEMA);

        //Validate candidates values
        switch (i) {
        case 1:
            assertThat(after.getInt16("c1")).isEqualTo((short)255);
            assertThat(after.getInt16("c2")).isEqualTo((short)(255));
            assertThat(after.getInt16("c3")).isEqualTo((short)127);
            break;
        case 2:
            assertThat(after.getInt16("c1")).isEqualTo((short)155);
            assertThat(after.getInt16("c2")).isEqualTo((short)155);
            assertThat(after.getInt16("c3")).isEqualTo((short)-100);
            break;
        case 3:
            assertThat(after.getInt16("c1")).isEqualTo((short)0);
            assertThat(after.getInt16("c2")).isEqualTo((short)0);
            assertThat(after.getInt16("c3")).isEqualTo((short)-128);
        }
    }

    private void assertSmallUnsigned(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        //Validate the schema first, we are expecting int-32 since we are dealing with unsignd-smallint
        //So Unsigned SMALLINT would be an int32 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(Schema.INT32_SCHEMA);

        //Validate the schema first, we are expecting int-16 since we are dealing with signed-smallint.
        //So Signed SMALLINT would be an INT16 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT16_SCHEMA);

        //Validate candidates values
        switch (i) {
        case 1:
            assertThat(after.getInt32("c1")).isEqualTo(65535);
            assertThat(after.getInt32("c2")).isEqualTo(65535);
            assertThat(after.getInt16("c3")).isEqualTo((short)32767);
            break;
        case 2:
            assertThat(after.getInt32("c1")).isEqualTo(45535);
            assertThat(after.getInt32("c2")).isEqualTo(45535);
            assertThat(after.getInt16("c3")).isEqualTo((short)-12767);
            break;
        case 3:
            assertThat(after.getInt32("c1")).isEqualTo(0);
            assertThat(after.getInt32("c2")).isEqualTo(0);
            assertThat(after.getInt16("c3")).isEqualTo((short)-32768);
        }
    }

    private void assertMediumUnsigned(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        //Validate the schema first, we are expecting int-32 since we are dealing with unsignd-mediumint
        //So Unsigned MEDIUMINT would be an int32 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(Schema.INT32_SCHEMA);

        //Validate the schema first, we are expecting int-32 since we are dealing with signed-mediumint.
        //So Signed MEDIUMINT would be an INT32 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT32_SCHEMA);

        //Validate candidates values
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
        //Validate the schema first, we are expecting int-64 since we are dealing with unsignd-int
        //So Unsigned INT would be an INT64 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(after.schema().field("c2").schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(after.schema().field("c4").schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(after.schema().field("c5").schema()).isEqualTo(Schema.INT64_SCHEMA);

        //Validate the schema first, we are expecting int-32 since we are dealing with signed-int
        //So Signed INT would be an INT32 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(after.schema().field("c6").schema()).isEqualTo(Schema.INT32_SCHEMA);

        //Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html

        //Validate candidates values
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

    private void assertBigintUnsigned(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        assertThat(i).isNotNull();
        //Validate the schema first, we are expecting org.apache.kafka.connect.data.Decimal:Byte  since we are dealing with unsignd-bigint
        //So Unsigned BIGINY would be an int32 type
        assertThat(after.schema().field("c1").schema()).isEqualTo(Decimal.builder(0).schema());
        assertThat(after.schema().field("c2").schema()).isEqualTo(Decimal.builder(0).schema());

        //Validate the schema first, we are expecting int-64 since we are dealing with signed-bigint.
        //So Signed BIGINT would be an INT64 type
        assertThat(after.schema().field("c3").schema()).isEqualTo(Schema.INT64_SCHEMA);

        //Validate candidates values
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
}
