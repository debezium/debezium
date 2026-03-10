/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;

/**
 * Tests around {@code NUMERIC} columns. Keep in sync with {@link BinlogDecimalColumnIT}.
 *
 * @author Gunnar Morling
 */
public abstract class BinlogNumericColumnIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-numeric-column.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("numericcolumnit", "numeric_column_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-751")
    public void shouldSetPrecisionSchemaParameter() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 1;
        int numInserts = 1;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numInserts);
        stopConnector();
        assertThat(records).isNotNull();
        records.forEach(this::validate);

        List<SourceRecord> dmls = records.recordsForTopic(DATABASE.topicForTable("dbz_751_numeric_column_test"));
        assertThat(dmls).hasSize(1);

        SourceRecord insert = dmls.get(0);

        Map<String, String> rating1SchemaParameters = insert.valueSchema()
                .field("before")
                .schema()
                .field("rating1")
                .schema()
                .parameters();

        assertThat(rating1SchemaParameters).contains(
                entry("scale", "0"), entry(PRECISION_PARAMETER_KEY, "10"));

        Map<String, String> rating2SchemaParameters = insert.valueSchema()
                .field("before")
                .schema()
                .field("rating2")
                .schema()
                .parameters();

        assertThat(rating2SchemaParameters).contains(
                entry("scale", "4"), entry(PRECISION_PARAMETER_KEY, "8"));

        Map<String, String> rating3SchemaParameters = insert.valueSchema()
                .field("before")
                .schema()
                .field("rating3")
                .schema()
                .parameters();

        assertThat(rating3SchemaParameters).contains(
                entry("scale", "0"), entry(PRECISION_PARAMETER_KEY, "7"));

        Map<String, String> rating4SchemaParameters = insert.valueSchema()
                .field("before")
                .schema()
                .field("rating4")
                .schema()
                .parameters();

        assertThat(rating4SchemaParameters).contains(
                entry("scale", "0"), entry(PRECISION_PARAMETER_KEY, "6"));
    }

    @Test
    public void testPreciseDecimalHandlingMode() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dbz_751_numeric_column_test"))
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE)
                .build();

        start(getConnectorClass(), config);

        assertBigDecimalChangeRecord(consumeInsert());

        stopConnector();
    }

    @Test
    public void testDoubleDecimalHandlingMode() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dbz_751_numeric_column_test"))
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                .build();

        start(getConnectorClass(), config);

        assertDoubleChangeRecord(consumeInsert());

        stopConnector();
    }

    @Test
    public void testStringDecimalHandlingMode() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dbz_751_numeric_column_test"))
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING)
                .build();

        start(getConnectorClass(), config);

        assertStringChangeRecord(consumeInsert());

        stopConnector();
    }

    private SourceRecord consumeInsert() throws InterruptedException {
        final int numDatabase = 2;
        final int numTables = 4;
        final int numOthers = 1;

        SourceRecords records = consumeRecordsByTopic(numDatabase + numTables + numOthers);

        assertThat(records).isNotNull();

        List<SourceRecord> events = records.recordsForTopic(DATABASE.topicForTable("dbz_751_numeric_column_test"));
        assertThat(events).hasSize(1);

        return events.get(0);
    }

    private void assertBigDecimalChangeRecord(SourceRecord record) {
        assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        assertThat(change.get("rating1")).isEqualTo(new BigDecimal("123"));
        assertThat(change.get("rating2")).isEqualTo(new BigDecimal("123.4567"));
        assertThat(change.get("rating3")).isEqualTo(new BigDecimal("235"));
        assertThat(change.get("rating4")).isEqualTo(new BigDecimal("346"));
    }

    private void assertDoubleChangeRecord(SourceRecord record) {
        assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        assertThat(change.getFloat64("rating1")).isEqualTo(123.0);
        assertThat(change.getFloat64("rating2")).isEqualTo(123.4567);
        assertThat(change.getFloat64("rating3")).isEqualTo(235.0);
        assertThat(change.getFloat64("rating4")).isEqualTo(346.0);
    }

    private void assertStringChangeRecord(SourceRecord record) {
        assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        assertThat(change.getString("rating1")).isEqualTo("123");
        assertThat(change.getString("rating2")).isEqualTo("123.4567");
        assertThat(change.getString("rating3")).isEqualTo("235");
        assertThat(change.getString("rating4")).isEqualTo("346");
    }
}
