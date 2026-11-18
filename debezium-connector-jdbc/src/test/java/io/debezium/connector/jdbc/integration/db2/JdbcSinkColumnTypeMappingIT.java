/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.db2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.Db2SinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;
import io.debezium.time.StructuredTimestamp;

@Tag("all")
@Tag("it")
@Tag("it-db2")
@ExtendWith(Db2SinkDatabaseContextProvider.class)
public class JdbcSinkColumnTypeMappingIT extends AbstractJdbcSinkTest {

    public JdbcSinkColumnTypeMappingIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2119")
    public void shouldPreservePicosecondTimestamp(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        final var schema = StructuredTimestamp.builder(12).build();
        final var value = StructuredTimestamp.fromPicoseconds(schema, 2026, 7, 17, 12, 13, 14, 123_456_789_012L, 12);
        final JdbcKafkaSinkRecord record = factory.createRecordWithSchemaValue(
                topicName, (byte) 1, "captured_at", schema, value, getConfig(properties));

        consume(record);

        final String destinationTable = destinationTableName(record);
        getSink().assertColumn(destinationTable, "captured_at", "TIMESTAMP");
        try (Statement statement = getSink().getConnection().createStatement();
                ResultSet resultSet = statement.executeQuery(
                        "SELECT CAST(captured_at AS VARCHAR(32)) FROM " + destinationTable)) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).endsWith("123456789012");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2119")
    public void shouldRejectStructuredTimestampRangeLoss(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        final var schema = StructuredTimestamp.builder(6).build();
        final var value = StructuredTimestamp.from(schema, 10_000, 1, 1, 0, 0, 0, 0, 6);
        final JdbcKafkaSinkRecord record = factory.createRecordWithSchemaValue(
                topicName, (byte) 1, "captured_at", schema, value, getConfig(properties));

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> consume(record));
        assertExceptionCauseMessage(exception, ".*outside the range.*");
    }
}
