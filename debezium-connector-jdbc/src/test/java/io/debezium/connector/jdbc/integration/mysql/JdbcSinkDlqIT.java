/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkTaskTestContext;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.MySqlSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * JDBC sink tests for routing non-retriable record failures to the errant record reporter (DLQ)
 * when the shared change event sink framework is enabled.
 */
@Tag("all")
@Tag("it")
@Tag("it-mysql")
@ExtendWith(MySqlSinkDatabaseContextProvider.class)
public class JdbcSinkDlqIT extends AbstractJdbcSinkTest {

    private final List<SinkRecord> reportedRecords = new ArrayList<>();

    public JdbcSinkDlqIT(Sink sink) {
        super(sink);
    }

    @BeforeEach
    void resetReportedRecords() {
        reportedRecords.clear();
    }

    @Override
    protected JdbcSinkTaskTestContext createTaskContext(Map<String, String> properties) {
        return new JdbcSinkTaskTestContext(properties) {
            @Override
            public ErrantRecordReporter errantRecordReporter() {
                return (record, error) -> {
                    reportedRecords.add(record);
                    return CompletableFuture.completedFuture(null);
                };
            }
        };
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#984")
    public void testShouldReportConstraintViolatingRecordAndContinue(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.ENABLE_SHARED_CHANGE_EVENT_SINK_FIELD.name(), "true");
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord record = factory.createRecord(topicName, (byte) 1, config);
        final String destinationTable = destinationTableName(record);

        consume(record);

        // The same key violates the primary key constraint in insert mode; the record must be
        // routed to the errant record reporter and the task must remain healthy.
        consume(factory.createRecord(topicName, (byte) 1, config));

        assertThat(reportedRecords).hasSize(1);

        // A subsequent healthy record must still be written.
        consume(factory.createRecord(topicName, (byte) 2, config));

        getSink().assertRows(destinationTable, rs -> {
            int rows = 1;
            while (rs.next()) {
                rows++;
            }
            assertThat(rows).isEqualTo(2);
            return null;
        });
        assertThat(reportedRecords).hasSize(1);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#984")
    public void testShouldFailTaskWithoutSharedChangeEventSink(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.ENABLE_SHARED_CHANGE_EVENT_SINK_FIELD.name(), "false");
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = getConfig(properties);
        consume(factory.createRecord(topicName, (byte) 1, config));

        // Without the shared change event sink framework, the legacy path fails the task
        // regardless of the errant record reporter being available.
        assertThatThrownBy(() -> consume(factory.createRecord(topicName, (byte) 1, config)))
                .isInstanceOf(RuntimeException.class);
        assertThat(reportedRecords).isEmpty();
    }
}
