/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Common tests for {@code binary.handling.mode} and its per-field selectors.
 *
 * @author Minjae Lee
 */
public abstract class AbstractJdbcSinkBinaryHandlingModeTest extends AbstractJdbcSinkTest {

    protected static final byte[] NON_UTF8_BYTES = { (byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0 };

    public AbstractJdbcSinkBinaryHandlingModeTest(Sink sink) {
        super(sink);
    }

    /**
     * The character column type used for encoded string landings, e.g. {@code text} or {@code varchar(64)}.
     */
    protected abstract String characterColumnType();

    /**
     * The binary column type used for raw byte landings, e.g. {@code bytea} or {@code varbinary(16)}.
     */
    protected abstract String binaryColumnType();

    /**
     * DDL for a table with a single {@code data} column of the given type; override for dialects
     * with a non-standard {@code CREATE TABLE} syntax.
     */
    protected String singleDataColumnTableDdl(String tableName, String dataColumnType) {
        return String.format("CREATE TABLE %s (id int not null, data %s, primary key(id))", tableName, dataColumnType);
    }

    /**
     * DDL for the selector test table with the {@code data_hex}, {@code data_b64}, and {@code data_raw}
     * columns; override for dialects with a non-standard {@code CREATE TABLE} syntax.
     */
    protected String selectorColumnsTableDdl(String tableName) {
        return String.format("CREATE TABLE %s (id int not null, data_hex %s, data_b64 %s, data_raw %s, primary key(id))",
                tableName, characterColumnType(), characterColumnType(), binaryColumnType());
    }

    /**
     * The common sink configuration used by the binary handling tests.
     */
    protected Map<String, String> binaryHandlingSinkConfig(String binaryHandlingMode) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.BINARY_HANDLING_MODE, binaryHandlingMode);
        return properties;
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testBytesFieldWithBase64ModeIsBoundAsStringToCharacterColumn(SinkRecordFactory factory) throws Exception {
        // ByteBuffer here and byte[] in the other modes so that both value forms are covered
        assertBytesLandsInCharacterColumn(factory, "base64", ByteBuffer.wrap(NON_UTF8_BYTES), "/9j/4A==");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testBytesFieldWithBase64UrlSafeModeIsBoundAsStringToCharacterColumn(SinkRecordFactory factory) throws Exception {
        assertBytesLandsInCharacterColumn(factory, "base64-url-safe", NON_UTF8_BYTES, "_9j_4A==");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testBytesFieldWithHexModeIsBoundAsStringToCharacterColumn(SinkRecordFactory factory) throws Exception {
        assertBytesLandsInCharacterColumn(factory, "hex", NON_UTF8_BYTES, "ffd8ffe0");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testBytesFieldWithTextualModeKeepsRawBytesForBinaryColumn(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = binaryHandlingSinkConfig("base64");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_BYTES_SCHEMA,
                ByteBuffer.wrap(NON_UTF8_BYTES),
                config);

        final String destinationTable = destinationTableName(createRecord);
        getSink().execute(singleDataColumnTableDdl(destinationTable, binaryColumnType()));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getBytes(2)).isEqualTo(NON_UTF8_BYTES);
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testSelectorsOverrideGlobalModePerField(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = binaryHandlingSinkConfig("base64");
        properties.put(JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_HEX, "data_hex");
        properties.put(JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_BYTES, "data_raw");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("data_hex", "data_b64", "data_raw"),
                List.of(Schema.OPTIONAL_BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA),
                List.of(NON_UTF8_BYTES, NON_UTF8_BYTES, NON_UTF8_BYTES),
                config);

        final String destinationTable = destinationTableName(createRecord);
        getSink().execute(selectorColumnsTableDdl(destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getString(2)).isEqualTo("ffd8ffe0");
            assertThat(rs.getString(3)).isEqualTo("/9j/4A==");
            assertThat(rs.getBytes(4)).isEqualTo(NON_UTF8_BYTES);
            return null;
        });
    }

    private void assertBytesLandsInCharacterColumn(SinkRecordFactory factory, String mode, Object value, String expected) throws Exception {
        final Map<String, String> properties = binaryHandlingSinkConfig(mode);
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_BYTES_SCHEMA,
                value,
                config);

        final String destinationTable = destinationTableName(createRecord);
        getSink().execute(singleDataColumnTableDdl(destinationTable, characterColumnType()));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getString(2)).isEqualTo(expected);
            return null;
        });
    }
}
