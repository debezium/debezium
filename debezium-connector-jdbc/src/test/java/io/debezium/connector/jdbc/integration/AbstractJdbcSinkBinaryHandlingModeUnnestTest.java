/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.UnnestRecordWriter;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;

import ch.qos.logback.classic.Level;

/**
 * Binary handling tests for the {@code UNNEST}-based batch write path, shared by the dialects that
 * derive from the PostgreSQL dialect.
 *
 * @author Minjae Lee
 */
public abstract class AbstractJdbcSinkBinaryHandlingModeUnnestTest extends AbstractJdbcSinkBinaryHandlingModeTest {

    private static final String UNNEST_EXECUTED_MESSAGE = "UNNEST batch insert affected";

    public AbstractJdbcSinkBinaryHandlingModeUnnestTest(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testEncodedBinaryFieldsUseUnnestBatchPath(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = unnestSinkConfig("hex");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = getConfig(properties);
        final List<JdbcKafkaSinkRecord> batch = List.of(
                createRecord(factory, topicName, (byte) 1, new byte[]{ (byte) 0xFF, (byte) 0xD8, (byte) 0xFF, 0x01 }, config),
                createRecord(factory, topicName, (byte) 2, new byte[]{ (byte) 0xFF, (byte) 0xD8, (byte) 0xFF, 0x02 }, config),
                createRecord(factory, topicName, (byte) 3, new byte[]{ (byte) 0xFF, (byte) 0xD8, (byte) 0xFF, 0x03 }, config));

        final String destinationTable = destinationTableName(batch.get(0));
        getSink().execute(singleDataColumnTableDdl(destinationTable, characterColumnType()));

        final LogInterceptor interceptor = new LogInterceptor(UnnestRecordWriter.class);
        interceptor.setLoggerLevel(UnnestRecordWriter.class, Level.DEBUG);

        consume(batch);

        getSink().assertRows(destinationTable, rs -> {
            final Map<Integer, String> rows = new HashMap<>();
            do {
                rows.put(rs.getInt(1), rs.getString(2));
            } while (rs.next());
            assertThat(rows).containsOnlyKeys(1, 2, 3);
            assertThat(rows.get(1)).isEqualTo("ffd8ff01");
            assertThat(rows.get(2)).isEqualTo("ffd8ff02");
            assertThat(rows.get(3)).isEqualTo("ffd8ff03");
            return null;
        });

        // The encoded string binding is array-compatible, so the batch stays on the UNNEST path
        assertThat(interceptor.containsMessage(UNNEST_EXECUTED_MESSAGE)).isTrue();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2468")
    public void testRawBinaryFieldsUseUnnestBatchPathAsTypedByteaArrays(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = unnestSinkConfig("base64");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = getConfig(properties);
        final List<JdbcKafkaSinkRecord> batch = List.of(
                createRecord(factory, topicName, (byte) 1, new byte[]{ 0x01, 0x02 }, config),
                createRecord(factory, topicName, (byte) 2, new byte[]{ 0x03, 0x04 }, config));

        // The destination column is binary, so the field keeps the raw bytes binding; the batch
        // stays on the UNNEST path with the values passed as a typed byte[][] array.
        final String destinationTable = destinationTableName(batch.get(0));
        getSink().execute(singleDataColumnTableDdl(destinationTable, binaryColumnType()));

        final LogInterceptor interceptor = new LogInterceptor(UnnestRecordWriter.class);
        interceptor.setLoggerLevel(UnnestRecordWriter.class, Level.DEBUG);

        consume(batch);

        getSink().assertRows(destinationTable, rs -> {
            final Map<Integer, byte[]> rows = new HashMap<>();
            do {
                rows.put(rs.getInt(1), rs.getBytes(2));
            } while (rs.next());
            assertThat(rows).containsOnlyKeys(1, 2);
            assertThat(rows.get(1)).isEqualTo(new byte[]{ 0x01, 0x02 });
            assertThat(rows.get(2)).isEqualTo(new byte[]{ 0x03, 0x04 });
            return null;
        });

        assertThat(interceptor.containsMessage(UNNEST_EXECUTED_MESSAGE)).isTrue();
    }

    private Map<String, String> unnestSinkConfig(String binaryHandlingMode) {
        final Map<String, String> properties = binaryHandlingSinkConfig(binaryHandlingMode);
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "true");
        return properties;
    }

    private JdbcKafkaSinkRecord createRecord(SinkRecordFactory factory, String topicName, byte key, byte[] value, JdbcSinkConnectorConfig config) {
        return factory.createRecordWithSchemaValue(topicName, key, "data", Schema.OPTIONAL_BYTES_SCHEMA, value, config);
    }
}
