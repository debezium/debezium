/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.junit.jupiter.StarRocksSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;

/**
 * Column type mappings for StarRocks, including boundary cases for byte-length string
 * semantics, unsigned 64-bit integers, maximum decimal precision, microsecond datetime
 * precision and the TIME-as-string representation.
 */
@Tag("all")
@Tag("it")
@Tag("it-starrocks")
@ExtendWith(StarRocksSinkDatabaseContextProvider.class)
public class JdbcSinkColumnTypeMappingIT extends AbstractJdbcSinkTest {

    public JdbcSinkColumnTypeMappingIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6967")
    public void testShouldCoerceNioByteBufferTypeToByteArrayColumnType(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        final ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put((byte) 1);
        buffer.put((byte) 2);
        buffer.put((byte) 3);

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_BYTES_SCHEMA,
                buffer,
                config);

        final String destinationTable = destinationTableName(createRecord);
        getSink().execute(String.format(
                "CREATE TABLE %s (id tinyint NOT NULL, data varbinary(3) NULL) PRIMARY KEY(id) DISTRIBUTED BY HASH(id)",
                destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getBytes(2)).isEqualTo(new byte[]{ 1, 2, 3 });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldPreserveMicrosecondDatetimePrecision(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        // 2024-01-02 03:04:05.123456 as microseconds past epoch
        final long micros = 1704164645123456L;

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                MicroTimestamp.builder().optional().build(),
                micros,
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertColumn(destinationTable, "data", "DATETIME");
        // The StarRocks JDBC driver reports DATETIME columns without fractional second
        // precision and truncates values on read; a cast to VARCHAR exposes the stored value.
        try (var connection = dataSource().getConnection();
                var statement = connection.createStatement();
                var rs = statement.executeQuery("SELECT CAST(data AS VARCHAR(32)) FROM " + destinationTable)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("2024-01-02 03:04:05.123456");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldWriteTimeValuesAsFormattedStrings(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        // 23:59:59.999999 as microseconds past midnight
        final long micros = ((23L * 3600 + 59 * 60 + 59) * 1_000_000) + 999_999;

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                MicroTime.builder().optional().build(),
                micros,
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        // StarRocks has no TIME data type; times are stored as ISO-8601 formatted strings
        getSink().assertColumn(destinationTable, "data", "VARCHAR");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getString("data")).isEqualTo("23:59:59.999999");
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldMapBigIntUnsignedToLargeInt(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        final Schema bigintUnsignedSchema = SchemaBuilder.int64()
                .optional()
                .parameter("__debezium.source.column.type", "BIGINT UNSIGNED")
                .build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                bigintUnsignedSchema,
                // 18446744073709551615 (2^64 - 1) wrapped into a signed long
                -1L,
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        // The column is created as LARGEINT, which StarRocks surfaces through JDBC metadata as
        // an unsigned bigint; the value assertion confirms the full unsigned range round-trips.
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getString("data")).isEqualTo("18446744073709551615");
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldRoundTripMaximumDecimalPrecision(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        // decimal(38,10) with all digits populated
        final BigDecimal value = new BigDecimal("1234567890123456789012345678.0123456789");
        final Schema decimalSchema = Decimal.builder(10)
                .optional()
                .parameter("connect.decimal.precision", "38")
                .build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                decimalSchema,
                value,
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getBigDecimal("data")).isEqualByComparingTo(value);
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldWidenCharacterLengthsForMultiByteData(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        // A VARCHAR(10) source column filled with 10 three-byte characters (30 bytes); StarRocks
        // VARCHAR lengths are bytes, so the sink must widen the declared length.
        final String value = "한".repeat(10);
        final Schema stringSchema = SchemaBuilder.string()
                .optional()
                .parameter("__debezium.source.column.type", "VARCHAR")
                .parameter("__debezium.source.column.length", "10")
                .build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                stringSchema,
                value,
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertColumn(destinationTable, "data", "VARCHAR", 40);
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getString("data")).isEqualTo(value);
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldWriteIntoPreCreatedTableWithStringColumn(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_STRING_SCHEMA,
                "한글 텍스트",
                config);

        final String destinationTable = destinationTableName(createRecord);
        // Users typically declare text columns with the idiomatic STRING alias when creating
        // StarRocks tables themselves; ensure the sink writes into such tables.
        getSink().execute(String.format(
                "CREATE TABLE %s (id tinyint NOT NULL, data string NULL) PRIMARY KEY(id) DISTRIBUTED BY HASH(id)",
                destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getString("data")).isEqualTo("한글 텍스트");
            return null;
        });

        // Upserting into the STRING column also replaces the row
        final JdbcKafkaSinkRecord updateRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_STRING_SCHEMA,
                "updated",
                config);
        consume(updateRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getString("data")).isEqualTo("updated");
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldPromoteWideCharColumnsToVarchar(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        final Schema smallCharSchema = SchemaBuilder.string()
                .optional()
                .parameter("__debezium.source.column.type", "CHAR")
                .parameter("__debezium.source.column.length", "10")
                .build();
        final Schema wideCharSchema = SchemaBuilder.string()
                .optional()
                .parameter("__debezium.source.column.type", "CHAR")
                .parameter("__debezium.source.column.length", "100")
                .build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("small_char", "wide_char"),
                List.of(smallCharSchema, wideCharSchema),
                List.of("한글", "한".repeat(100)),
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        // Byte-scaled CHAR lengths that fit within StarRocks' 255 byte CHAR maximum stay CHAR;
        // wider columns are promoted to VARCHAR.
        getSink().assertColumn(destinationTable, "small_char", "CHAR", 40);
        getSink().assertColumn(destinationTable, "wide_char", "VARCHAR", 400);
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getString("small_char")).isEqualTo("한글");
            assertThat(rs.getString("wide_char")).isEqualTo("한".repeat(100));
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldMapFloatingPointTypes(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("float_data", "double_data"),
                List.of(Schema.OPTIONAL_FLOAT32_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA),
                List.of(3.14f, 2.718281828459045d),
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        // StarRocks rejects float(p) and "double precision"; the dialect emits the plain names
        getSink().assertColumn(destinationTable, "float_data", "FLOAT");
        getSink().assertColumn(destinationTable, "double_data", "DOUBLE");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getFloat("float_data")).isEqualTo(3.14f);
            assertThat(rs.getDouble("double_data")).isEqualTo(2.718281828459045d);
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldMapBooleanToNativeBooleanType(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_BOOLEAN_SCHEMA,
                Boolean.TRUE,
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getBoolean("data")).isTrue();
            return null;
        });
    }
}
