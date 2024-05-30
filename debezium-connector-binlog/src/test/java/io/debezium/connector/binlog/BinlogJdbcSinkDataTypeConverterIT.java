/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.converters.JdbcSinkDataTypesConverter;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;

/**
 * Tests for the Binlog-based connector JDBC Sink converter.
 *
 * @author Chris Cranford
 */
public abstract class BinlogJdbcSinkDataTypeConverterIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-jdbc-sink.text").toAbsolutePath();

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-6225")
    public void testBooleanDataTypeMapping() throws Exception {
        // TODO: remove once we upgrade Apicurio version (DBZ-7357)
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation();
        }

        final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("booleanit", "boolean_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);
        DATABASE.createAndInitialize();
        Files.delete(SCHEMA_HISTORY_PATH);

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("BOOLEAN_TEST") + "," + DATABASE.qualifiedTableName("BOOLEAN_TEST2"))
                .with(BinlogConnectorConfig.PROPAGATE_COLUMN_SOURCE_TYPE, ".*")
                .with(BinlogConnectorConfig.CUSTOM_CONVERTERS, "jdbc-sink")
                .with("jdbc-sink.type", JdbcSinkDataTypesConverter.class.getName())
                .with("jdbc-sink.selector.boolean", ".*BOOLEAN_TEST.b.*|.*BOOLEAN_TEST2.b.*")
                .build();

        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(2 + 4 + 1);
        assertThat(records).isNotNull();

        List<SourceRecord> tableRecords = records.recordsForTopic(DATABASE.topicForTable("BOOLEAN_TEST"));
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);
        System.out.println(record);

        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        Schema afterSchema = record.valueSchema().field("after").schema();

        // Assert how the BOOLEAN data type is mapped during the snapshot phase.
        assertThat(afterSchema.field("b1").schema().type()).isEqualTo(Schema.Type.INT16);
        assertThat(afterSchema.field("b1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("TINYINT");
        assertThat(afterSchema.field("b1").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");
        assertThat(after.get("b1")).isEqualTo((short) 0);
        assertThat(afterSchema.field("b2").schema().type()).isEqualTo(Schema.Type.INT16);
        assertThat(afterSchema.field("b2").schema().parameters().get("__debezium.source.column.type")).isEqualTo("TINYINT");
        assertThat(afterSchema.field("b2").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");
        assertThat(after.get("b2")).isEqualTo((short) 1);

        // Create the table after-the-fact
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection conn = db.connect()) {
                conn.execute("CREATE TABLE BOOLEAN_TEST2 (`id` INT NOT NULL AUTO_INCREMENT, " +
                        "`b1` boolean default true, `b2` boolean default false, " +
                        "primary key (`ID`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;");
                conn.execute("INSERT INTO BOOLEAN_TEST2 (b1,b2) VALUES (true, false)");
            }
        }

        records = consumeRecordsByTopic(2);
        assertThat(records).isNotNull();

        tableRecords = records.recordsForTopic(DATABASE.topicForTable("BOOLEAN_TEST2"));
        assertThat(tableRecords).hasSize(1);

        record = tableRecords.get(0);

        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        afterSchema = record.valueSchema().field("after").schema();

        // Assert how the BOOLEAN data type is mapped during the streaming phase.
        // During streaming the DDL that gets parsed provides the column type as BOOLEAN and this is what gets passed
        // into the Column's relational model and gets propagated. Despite being BOOLEAN, it should still be sent as
        // an INT16 data type into Kafka. The sink connector should be able to deduce the type as TINYINT(1) when the
        // column propagation is enabled because of type being BOOLEAN.
        assertThat(afterSchema.field("b1").schema().type()).isEqualTo(Schema.Type.INT16);
        assertThat(afterSchema.field("b1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("BOOLEAN");
        assertThat(afterSchema.field("b1").schema().parameters().get("__debezium.source.column.length")).isNull();
        assertThat(after.get("b1")).isEqualTo((short) 1);
        assertThat(afterSchema.field("b2").schema().type()).isEqualTo(Schema.Type.INT16);
        assertThat(afterSchema.field("b2").schema().parameters().get("__debezium.source.column.type")).isEqualTo("BOOLEAN");
        assertThat(afterSchema.field("b2").schema().parameters().get("__debezium.source.column.length")).isNull();
        assertThat(after.get("b2")).isEqualTo((short) 0);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-6226")
    public void testRealDataTypeMapping() throws Exception {
        final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("realit", "real_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);
        DATABASE.createAndInitialize();
        Files.delete(SCHEMA_HISTORY_PATH);

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("REAL_TEST") + "," + DATABASE.qualifiedTableName("REAL_TEST2"))
                .with(BinlogConnectorConfig.PROPAGATE_COLUMN_SOURCE_TYPE, ".*")
                .with(BinlogConnectorConfig.CUSTOM_CONVERTERS, "jdbc-sink")
                .with("jdbc-sink.type", JdbcSinkDataTypesConverter.class.getName())
                .with("jdbc-sink.selector.real", ".*REAL_TEST.r.*|.*REAL_TEST2.r.*")
                .build();

        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(2 + 4 + 1);
        assertThat(records).isNotNull();

        List<SourceRecord> tableRecords = records.recordsForTopic(DATABASE.topicForTable("REAL_TEST"));
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);

        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        Schema afterSchema = record.valueSchema().field("after").schema();

        // Assert how the BOOLEAN data type is mapped during the snapshot phase.
        assertThat(afterSchema.field("r1").schema().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(afterSchema.field("r1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("DOUBLE");
        assertThat(after.get("r1")).isEqualTo(2.36d);

        // Create the table after-the-fact
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection conn = db.connect()) {
                conn.execute("CREATE TABLE REAL_TEST2 (`id` INT NOT NULL AUTO_INCREMENT, " +
                        "`r1` real default 3.14, " +
                        "primary key (`ID`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;");
                conn.execute("INSERT INTO REAL_TEST2 (r1) VALUES (9.78)");
            }
        }

        records = consumeRecordsByTopic(2);
        assertThat(records).isNotNull();

        tableRecords = records.recordsForTopic(DATABASE.topicForTable("REAL_TEST2"));
        assertThat(tableRecords).hasSize(1);

        record = tableRecords.get(0);

        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        afterSchema = record.valueSchema().field("after").schema();

        // Assert the created table after-the-fact record is identical to the snapshot
        // During streaming the DDL that gets parsed provides the column type as REAL and this is what gets passed
        // into the Column's relational model and gets propagated. Despite being REAL, it should still be sent as
        // a FLOAT64 data type into Kafka.
        assertThat(afterSchema.field("r1").schema().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(afterSchema.field("r1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("REAL");
        assertThat(after.get("r1")).isEqualTo(9.78d);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-6231")
    public void testNationalizedCharacterDataTypeMappings() throws Exception {
        final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("nctestit", "nationalized_character_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);
        DATABASE.createAndInitialize();
        Files.delete(SCHEMA_HISTORY_PATH);

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("NC_TEST") + "," + DATABASE.qualifiedTableName("NC_TEST2"))
                .with(BinlogConnectorConfig.PROPAGATE_COLUMN_SOURCE_TYPE, ".*")
                .with(BinlogConnectorConfig.CUSTOM_CONVERTERS, "jdbc-sink")
                .with("jdbc-sink.type", JdbcSinkDataTypesConverter.class.getName())
                .with("jdbc-sink.selector.string", ".*NC_TEST.nc.*|.*NC_TEST2.nc.*")
                .build();

        start(getConnectorClass(), config);

        SourceRecords records = consumeRecordsByTopic(2 + 4 + 1);
        assertThat(records).isNotNull();

        List<SourceRecord> tableRecords = records.recordsForTopic(DATABASE.topicForTable("NC_TEST"));
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);

        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        Schema afterSchema = record.valueSchema().field("after").schema();

        // Assert how the BOOLEAN data type is mapped during the snapshot phase.
        assertThat(afterSchema.field("nc1").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.get("nc1")).isEqualTo("a");
        assertThat(afterSchema.field("nc1").schema().parameters().get("__debezium.source.column.character_set")).startsWith("utf8");
        assertThat(afterSchema.field("nc1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("CHAR");
        assertThat(afterSchema.field("nc1").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");
        assertThat(afterSchema.field("nc2").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.get("nc2")).isEqualTo("123");
        assertThat(afterSchema.field("nc2").schema().parameters().get("__debezium.source.column.character_set")).startsWith("utf8");
        assertThat(afterSchema.field("nc2").schema().parameters().get("__debezium.source.column.type")).isEqualTo("CHAR");
        assertThat(afterSchema.field("nc2").schema().parameters().get("__debezium.source.column.length")).isEqualTo("5");
        assertThat(afterSchema.field("nc3").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.get("nc3")).isEqualTo("hello");
        assertThat(afterSchema.field("nc3").schema().parameters().get("__debezium.source.column.character_set")).startsWith("utf8");
        assertThat(afterSchema.field("nc3").schema().parameters().get("__debezium.source.column.type")).isEqualTo("VARCHAR");
        assertThat(afterSchema.field("nc3").schema().parameters().get("__debezium.source.column.length")).isEqualTo("25");

        // Create the table after-the-fact
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection conn = db.connect()) {
                conn.execute("CREATE TABLE NC_TEST2 (`id` INT NOT NULL AUTO_INCREMENT, " +
                        "`nc1` nchar, `nc2` nchar(5), `nc3` nvarchar(25), " +
                        "primary key (`ID`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;");
                conn.execute("INSERT INTO NC_TEST2 (nc1,nc2,nc3) VALUES ('b', '456', 'world')");
            }
        }

        records = consumeRecordsByTopic(2);
        assertThat(records).isNotNull();

        tableRecords = records.recordsForTopic(DATABASE.topicForTable("NC_TEST2"));
        assertThat(tableRecords).hasSize(1);

        record = tableRecords.get(0);

        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        afterSchema = record.valueSchema().field("after").schema();

        // Assert the created table after-the-fact record is identical to the snapshot
        // During streaming the DDL that gets parsed provides the column type as NCHAR or NVARCHAR as this gets pulled
        // directly from the DDL and this is what gets passed into the Column's relational model and gets propagated.
        // Using the converter, regardless of whether this is propagated or not, the character_set will be sent and the
        // sink will be able to use this information to derived nationalized character types when sourcing from MySQL.
        // Since these were always being mapped as Kafka STRING regardless, there is no issue with the schema types.
        assertThat(afterSchema.field("nc1").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.get("nc1")).isEqualTo("b");
        assertThat(afterSchema.field("nc1").schema().parameters().get("__debezium.source.column.character_set")).startsWith("utf8");
        assertThat(afterSchema.field("nc1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("NCHAR");
        assertThat(afterSchema.field("nc1").schema().parameters().get("__debezium.source.column.length")).isEqualTo("1");
        assertThat(afterSchema.field("nc2").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.get("nc2")).isEqualTo("456");
        assertThat(afterSchema.field("nc2").schema().parameters().get("__debezium.source.column.character_set")).startsWith("utf8");
        assertThat(afterSchema.field("nc2").schema().parameters().get("__debezium.source.column.type")).isEqualTo("NCHAR");
        assertThat(afterSchema.field("nc2").schema().parameters().get("__debezium.source.column.length")).isEqualTo("5");
        assertThat(afterSchema.field("nc3").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.get("nc3")).isEqualTo("world");
        assertThat(afterSchema.field("nc3").schema().parameters().get("__debezium.source.column.character_set")).startsWith("utf8");
        assertThat(afterSchema.field("nc3").schema().parameters().get("__debezium.source.column.type")).isEqualTo("NVARCHAR");
        assertThat(afterSchema.field("nc3").schema().parameters().get("__debezium.source.column.length")).isEqualTo("25");

        stopConnector();
    }

}
