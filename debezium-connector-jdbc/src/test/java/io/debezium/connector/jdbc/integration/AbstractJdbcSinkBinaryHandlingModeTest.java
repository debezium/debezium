/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Assumptions;
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
     * The large character column type used to verify stream bindings. Dialects that return
     * {@code null} do not run this test.
     */
    protected String largeCharacterColumnType() {
        return null;
    }

    /**
     * The national character column type, e.g. {@code nvarchar(max)} or {@code nvarchar2(64)}.
     * Dialects that return {@code null} do not run this test.
     */
    protected String nationalCharacterColumnType() {
        return null;
    }

    /**
     * The fixed-length character column type used to verify landings that the destination pads.
     */
    protected String fixedLengthCharacterColumnType() {
        return "char(16)";
    }

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
    public void testNamedRawBytesFieldIsBoundAsStringToCharacterColumn(SinkRecordFactory factory) throws Exception {
        final Schema namedBytesSchema = SchemaBuilder.bytes().name("com.example.Binary").optional().build();
        assertBytesLandsInCharacterColumn(factory, "hex", namedBytesSchema, NON_UTF8_BYTES, "ffd8ffe0");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testBytesFieldIsBoundAsStringToNationalCharacterColumn(SinkRecordFactory factory) throws Exception {
        final String nationalColumnType = nationalCharacterColumnType();
        Assumptions.assumeTrue(nationalColumnType != null, "Dialect does not define a national character column type for this test");
        assertBytesLandsInColumn(factory, "hex", nationalColumnType, Schema.OPTIONAL_BYTES_SCHEMA, NON_UTF8_BYTES, "ffd8ffe0", false);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testBytesFieldIsBoundAsStringToFixedLengthCharacterColumn(SinkRecordFactory factory) throws Exception {
        // Whether the padding of a fixed-length column is returned varies per database, so the
        // landed value is compared without its trailing padding; encoded values never end in spaces.
        assertBytesLandsInColumn(factory, "hex", fixedLengthCharacterColumnType(), Schema.OPTIONAL_BYTES_SCHEMA, NON_UTF8_BYTES, "ffd8ffe0", true);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testLargeAndNullBytesValuesAreBoundToLargeCharacterColumn(SinkRecordFactory factory) throws Exception {
        final String largeColumnType = largeCharacterColumnType();
        Assumptions.assumeTrue(largeColumnType != null, "Dialect does not define a large character column type for this test");

        final byte[] largeBytes = new byte[64 * 1024];
        for (int index = 0; index < largeBytes.length; index++) {
            largeBytes[index] = (byte) (index % 251);
        }
        final String expected = Base64.getEncoder().encodeToString(largeBytes);

        final Map<String, String> properties = binaryHandlingSinkConfig("base64");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        final JdbcSinkConnectorConfig config = getConfig(properties);
        final List<JdbcKafkaSinkRecord> records = List.of(
                factory.createRecordWithSchemaValue(topicName, (byte) 1, "data", Schema.OPTIONAL_BYTES_SCHEMA, largeBytes, config),
                factory.createRecordWithSchemaValue(topicName, (byte) 2, "data", Schema.OPTIONAL_BYTES_SCHEMA, null, config));

        final String destinationTable = destinationTableName(records.get(0));
        getSink().execute(singleDataColumnTableDdl(destinationTable, largeColumnType));

        consume(records);

        getSink().assertRows(destinationTable, rs -> {
            final Map<Integer, String> rows = new HashMap<>();
            do {
                rows.put(rs.getInt(1), rs.getString(2));
            } while (rs.next());
            assertThat(rows).containsOnlyKeys(1, 2);
            assertThat(rows.get(1)).isEqualTo(expected);
            assertThat(rows.get(2)).isNull();
            return null;
        });
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
    public void testTopicQualifiedModesUseSeparateRowWiseBatchesWithEncodedRecordsFirst(SinkRecordFactory factory) throws Exception {
        assertTopicQualifiedModesUseSeparateRowWiseBatches(factory, true);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testTopicQualifiedModesUseSeparateRowWiseBatchesWithRawRecordsFirst(SinkRecordFactory factory) throws Exception {
        assertTopicQualifiedModesUseSeparateRowWiseBatches(factory, false);
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
        assertBytesLandsInCharacterColumn(factory, mode, Schema.OPTIONAL_BYTES_SCHEMA, value, expected);
    }

    private void assertBytesLandsInCharacterColumn(SinkRecordFactory factory, String mode, Schema fieldSchema, Object value, String expected) throws Exception {
        assertBytesLandsInColumn(factory, mode, characterColumnType(), fieldSchema, value, expected, false);
    }

    private void assertBytesLandsInColumn(SinkRecordFactory factory, String mode, String columnType, Schema fieldSchema, Object value, String expected,
                                          boolean ignoreTrailingPadding)
            throws Exception {
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
                fieldSchema,
                value,
                config);

        final String destinationTable = destinationTableName(createRecord);
        getSink().execute(singleDataColumnTableDdl(destinationTable, columnType));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            final String landed = rs.getString(2);
            assertThat(ignoreTrailingPadding ? landed.stripTrailing() : landed).isEqualTo(expected);
            return null;
        });
    }

    private void assertTopicQualifiedModesUseSeparateRowWiseBatches(SinkRecordFactory factory, boolean encodedRecordsFirst) throws Exception {
        final String tableName = randomTableName();
        final String encodedTopic = "encoded_" + tableName;
        final String rawTopic = "raw_" + tableName;

        final Map<String, String> properties = binaryHandlingSinkConfig("bytes");
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, tableName);
        properties.put(JdbcSinkConnectorConfig.BINARY_HANDLING_SELECTOR_HEX, Pattern.quote(encodedTopic) + ":data");
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "false");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final JdbcSinkConnectorConfig config = getConfig(properties);
        final List<JdbcKafkaSinkRecord> encodedRecords = List.of(
                createRecord(factory, encodedTopic, (byte) 1, new byte[]{ 0x01, 0x11 }, config),
                createRecord(factory, encodedTopic, (byte) 2, new byte[]{ 0x02, 0x22 }, config));
        final List<JdbcKafkaSinkRecord> rawRecords = List.of(
                createRecord(factory, rawTopic, (byte) 3, new byte[]{ 0x41, 0x31 }, config),
                createRecord(factory, rawTopic, (byte) 4, new byte[]{ 0x42, 0x32 }, config));
        final List<JdbcKafkaSinkRecord> batch = new ArrayList<>();
        if (encodedRecordsFirst) {
            batch.addAll(encodedRecords);
            batch.addAll(rawRecords);
        }
        else {
            batch.addAll(rawRecords);
            batch.addAll(encodedRecords);
        }

        final String destinationTable = getSink().formatTableName(tableName);
        getSink().execute(singleDataColumnTableDdl(destinationTable, characterColumnType()));

        consume(batch);

        getSink().assertRows(destinationTable, rs -> {
            final Map<Integer, String> rows = new HashMap<>();
            do {
                rows.put(rs.getInt(1), rs.getString(2));
            } while (rs.next());
            assertThat(rows).containsOnlyKeys(1, 2, 3, 4);
            assertThat(rows.get(1)).isEqualTo("0111");
            assertThat(rows.get(2)).isEqualTo("0222");
            // Raw bytes targeting a character column retain the dialect's existing conversion behavior.
            assertThat(rows.get(3)).isNotNull();
            assertThat(rows.get(4)).isNotNull();
            return null;
        });
    }

    private JdbcKafkaSinkRecord createRecord(SinkRecordFactory factory, String topicName, byte key, byte[] value, JdbcSinkConnectorConfig config) {
        return factory.createRecordWithSchemaValue(topicName, key, "data", Schema.OPTIONAL_BYTES_SCHEMA, value, config);
    }
}
