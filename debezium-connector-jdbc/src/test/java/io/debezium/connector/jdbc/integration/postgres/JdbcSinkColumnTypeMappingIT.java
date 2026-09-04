/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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
import io.debezium.connector.jdbc.junit.jupiter.PostgresInsertModeArgumentsProvider;
import io.debezium.connector.jdbc.junit.jupiter.PostgresInsertModeArgumentsProvider.PostgresInsertMode;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.doc.FixFor;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTimestamp;

/**
 * Column type mapping tests for PostgreSQL.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkColumnTypeMappingIT extends AbstractJdbcSinkTest {

    public JdbcSinkColumnTypeMappingIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-6589")
    public void testShouldCoerceStringTypeToUuidColumnType(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        shouldCoerceStringTypeToColumnType(factory, insertMode, "uuid", "9bc6a215-84b5-4865-a058-9156427c887a", "f54c2926-076a-4db0-846f-14cad99a8307");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-6589")
    public void testShouldCoerceStringTypeToJsonColumnType(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        shouldCoerceStringTypeToColumnType(factory, insertMode, "json", "{\"id\": 12345}", "{\"id\": 67890}");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-6589")
    public void testShouldCoerceStringTypeToJsonbColumnType(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        shouldCoerceStringTypeToColumnType(factory, insertMode, "jsonb", "{\"id\": 12345}", "{\"id\": 67890}");
    }

    private void shouldCoerceStringTypeToColumnType(SinkRecordFactory factory, PostgresInsertMode insertMode, String columnType, String insertValue,
                                                    String updateValue)
            throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "false");
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_STRING_SCHEMA,
                insertValue,
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data %s null, primary key(id))";
        getSink().execute(String.format(sql, destinationTable, columnType));

        consume(createRecord);

        final JdbcKafkaSinkRecord updateRecord = factory.updateRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_STRING_SCHEMA,
                updateValue,
                config);

        consume(updateRecord);

        getSink().assertColumn(destinationTable, "data", columnType);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-6967")
    public void testShouldCoerceNioByteBufferTypeToByteArrayColumnType(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put((byte) 1);
        buffer.put((byte) 2);
        buffer.put((byte) 3);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_BYTES_SCHEMA,
                buffer,
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data bytea, primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getBytes(2)).isEqualTo(new byte[]{ 1, 2, 3 });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2119")
    public void testShouldCreateAndWriteStructuredTemporalColumnTypes(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);
        final Schema timestampSchema = StructuredTimestamp.schema();
        final Schema dateSchema = StructuredDate.schema();
        final Schema zonedTimestampSchema = StructuredZonedTimestamp.schema();
        final Schema durationSchema = StructuredDuration.schema();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of(
                        "timestamp_max",
                        "timestamp_pos_inf",
                        "timestamp_neg_inf",
                        "date_pos_inf",
                        "date_neg_inf",
                        "timestamptz_pos_inf",
                        "timestamptz_neg_inf",
                        "interval_value"),
                List.of(
                        timestampSchema,
                        timestampSchema,
                        timestampSchema,
                        dateSchema,
                        dateSchema,
                        zonedTimestampSchema,
                        zonedTimestampSchema,
                        durationSchema),
                List.of(
                        StructuredTimestamp.from(timestampSchema, 294276, 12, 31, 23, 59, 59, 999_999_000),
                        StructuredTimestamp.positiveInfinity(timestampSchema),
                        StructuredTimestamp.negativeInfinity(timestampSchema),
                        StructuredDate.positiveInfinity(dateSchema),
                        StructuredDate.negativeInfinity(dateSchema),
                        StructuredZonedTimestamp.positiveInfinity(zonedTimestampSchema),
                        StructuredZonedTimestamp.negativeInfinity(zonedTimestampSchema),
                        StructuredDuration.from(durationSchema, 1, 2, 3, 4, 5, 6, 789_000_000)),
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getString(2)).isEqualTo("294276-12-31 23:59:59.999999");
            assertThat(rs.getString(3)).isEqualTo("infinity");
            assertThat(rs.getString(4)).isEqualTo("-infinity");
            assertThat(rs.getString(5)).isEqualTo("infinity");
            assertThat(rs.getString(6)).isEqualTo("-infinity");
            assertThat(rs.getString(7)).isEqualTo("infinity");
            assertThat(rs.getString(8)).isEqualTo("-infinity");
            assertThat(rs.getString(9)).isEqualTo("1 year 2 mons 3 days 04:05:06.789");
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-7752")
    public void testShouldWorkWithTextArrayWithASingleValue(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                Arrays.asList("a"),
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data text[], primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getArray(2).getArray()).isEqualTo(new String[]{ "a" });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-7752")
    public void testShouldWorkWithTextArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                Arrays.asList("a", "b", "c"),
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data text[], primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getArray(2).getArray()).isEqualTo(new String[]{ "a", "b", "c" });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-7752")
    public void testShouldWorkWithTextArrayWithNullValues(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                Arrays.asList("a", null, "c", null),
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (data text[], id int not null, primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getArray(1).getArray()).isEqualTo(new String[]{ "a", null, "c", null });
            assertThat(rs.getInt(2)).isEqualTo(1);
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-7752")
    public void testShouldWorkWithNullTextArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                null,
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (data text[], id int not null, primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getArray(1)).isNull();
            assertThat(rs.getInt(2)).isEqualTo(1);

            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-7752")
    public void testShouldWorkWithEmptyArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                List.of(),
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data text[], primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getArray(2).getArray()).isEqualTo(new String[]{});
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-7752")
    public void testShouldWorkWithCharacterVaryingArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                Arrays.asList("a", "b", "c"),
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data character varying[], primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getArray(2).getArray()).isEqualTo(new String[]{ "a", "b", "c" });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-7752")
    public void testShouldWorkWithIntArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
                Arrays.asList(1, 2, 42),
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data int[], primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getArray(2).getArray()).isEqualTo(new Integer[]{ 1, 2, 42 });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-7752")
    public void testShouldWorkWithBoolArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).optional().build(),
                Arrays.asList(false, true),
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data bool[], primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getArray(2).getArray()).isEqualTo(new Boolean[]{ false, true });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("DBZ-7938")
    public void testShouldWorkWithMultipleArraysWithDifferentTypes(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);
        final List<UUID> uuids = List.of(UUID.randomUUID(), UUID.randomUUID());

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("text_data", "uuid_data"),
                List.of(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), SchemaBuilder.array(Uuid.schema()).optional().build()),
                Arrays.asList(List.of("a", "b"), uuids.stream().map(UUID::toString).collect(Collectors.toList())),
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, text_data text[], uuid_data uuid[], primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getArray(2).getArray()).isEqualTo(new String[]{ "a", "b" });
            assertThat(rs.getArray(3).getArray()).isEqualTo(uuids.toArray());
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testShouldWorkWithHstoreContainingQuotesBackslashesAndNulls(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        // A MAP whose keys/values contain the HSTORE special characters (double quote, backslash) as
        // well as a null value, all of which must be escaped or rendered as the NULL keyword.
        final Map<String, String> data = new LinkedHashMap<>();
        data.put("quote", "a \" b");
        data.put("back\\slash", "c:\\tmp");
        data.put("nullkey", null);

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                data,
                config);

        final String destinationTable = destinationTableName(createRecord);
        getSink().execute("CREATE EXTENSION IF NOT EXISTS hstore");
        final String sql = "CREATE TABLE %s (id int not null, data hstore, primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getString(2)).contains("\"quote\"=>\"a \\\" b\"");
            assertThat(rs.getString(2)).contains("\"back\\\\slash\"=>\"c:\\\\tmp\"");
            assertThat(rs.getString(2)).contains("\"nullkey\"=>NULL");
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testShouldCoerceStringTypeToMacaddr8ColumnType(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        // The source column type parameter drives the sink to map the plain STRING schema to a native
        // macaddr8 column (schema evolution) and to bind the value with cast(? as macaddr8).
        final Schema macaddr8Schema = SchemaBuilder.string()
                .optional()
                .parameter("__debezium.source.column.type", "MACADDR8")
                .build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                macaddr8Schema,
                "08:00:2b:01:02:03:04:05",
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertColumn(destinationTable, "data", "MACADDR8");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getString(2)).isEqualTo("08:00:2b:01:02:03:04:05");
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testShouldWorkWithNumericArrayWithPrecisionAndScale(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        // A numeric(10,2)[] element schema carries the precision/scale, so the element type name is
        // decimal(10,2). createArrayOf only accepts the base type name (numeric/decimal), so the
        // precision/scale must be stripped before binding the array.
        final Schema numericElementSchema = Decimal.builder(2)
                .optional()
                .parameter("connect.decimal.precision", "10")
                .build();

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                SchemaBuilder.array(numericElementSchema).optional().build(),
                Arrays.asList(new BigDecimal("1.25"), new BigDecimal("2.50"), new BigDecimal("-9999999.99")),
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data numeric(10,2)[], primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getArray(2).getArray()).isEqualTo(new BigDecimal[]{
                    new BigDecimal("1.25"), new BigDecimal("2.50"), new BigDecimal("-9999999.99") });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testShouldWorkWithInetArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        assertNativeArrayRoundTrip(factory, insertMode, "_INET", Schema.OPTIONAL_STRING_SCHEMA, "inet[]",
                Arrays.asList("192.168.1.1", "10.0.0.5"),
                new String[]{ "192.168.1.1", "10.0.0.5" });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testShouldWorkWithCidrArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        assertNativeArrayRoundTrip(factory, insertMode, "_CIDR", Schema.OPTIONAL_STRING_SCHEMA, "cidr[]",
                Arrays.asList("192.168.100.128/25", "10.0.0.0/8"),
                new String[]{ "192.168.100.128/25", "10.0.0.0/8" });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testShouldWorkWithMacaddrArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        assertNativeArrayRoundTrip(factory, insertMode, "_MACADDR", Schema.OPTIONAL_STRING_SCHEMA, "macaddr[]",
                Arrays.asList("08:00:2b:01:02:03", "08:00:2b:01:02:04"),
                new String[]{ "08:00:2b:01:02:03", "08:00:2b:01:02:04" });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testShouldWorkWithTsrangeArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        assertNativeArrayRoundTrip(factory, insertMode, "_TSRANGE", Schema.OPTIONAL_STRING_SCHEMA, "tsrange[]",
                Arrays.asList("[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")"),
                new String[]{ "[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")" });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testShouldWorkWithJsonbArray(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        assertNativeArrayRoundTrip(factory, insertMode, "_JSONB", Json.builder().optional().build(), "jsonb[]",
                Arrays.asList("{\"a\": 1}", "[1, 2, 3]"),
                new String[]{ "{\"a\": 1}", "[1, 2, 3]" });
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testShouldEvolveNativeArrayColumnToNativeType(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        // With schema evolution on, the sink auto-creates the column: _inet (inet[]) after the fix, _text before.
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        final Schema arraySchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .parameter("__debezium.source.column.type", "_INET")
                .build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName, (byte) 1, "data", arraySchema, Arrays.asList("192.168.1.1", "10.0.0.5"), config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertColumn(destinationTable, "data", "_inet");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            final Object[] actual = (Object[]) rs.getArray(2).getArray();
            assertThat(Arrays.stream(actual).map(String::valueOf).toArray(String[]::new))
                    .isEqualTo(new String[]{ "192.168.1.1", "10.0.0.5" });
            return null;
        });
    }

    /**
     * Round-trips a record into a pre-created native array column ({@code schema.evolution=none}), with the
     * array type carried on the field's {@code __debezium.source.column.type} as the source emits it (e.g. {@code _INET}).
     */
    private void assertNativeArrayRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode,
                                            String sourceColumnType, Schema elementSchema, String columnType,
                                            List<?> values, String[] expected)
            throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        final Schema arraySchema = SchemaBuilder.array(elementSchema)
                .optional()
                .parameter("__debezium.source.column.type", sourceColumnType)
                .build();

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName, (byte) 1, "data", arraySchema, values, config);

        final String destinationTable = destinationTableName(createRecord);
        getSink().execute(String.format("CREATE TABLE %s (id int not null, data %s, primary key(id))",
                destinationTable, columnType));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            // Native types such as inet/macaddr come back as driver-specific objects (PGobject) rather
            // than String, so compare on their textual form which is stable across both representations.
            final Object[] actual = (Object[]) rs.getArray(2).getArray();
            assertThat(Arrays.stream(actual).map(String::valueOf).toArray(String[]::new)).isEqualTo(expected);
            return null;
        });
    }

}
