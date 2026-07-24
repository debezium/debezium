/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.data.Schema;
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
    public void testShouldNotReportTransientFailuresAfterRetriesAreExhausted(SinkRecordFactory factory) throws SQLException {
        final String tableName = randomTableName();
        final Connection connection = getSink().getConnection();

        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE `" + tableName + "` (`id` bigint(20) NOT NULL, `content` varchar(200) DEFAULT NULL, PRIMARY KEY (`id`))");
            st.execute("INSERT INTO " + tableName + " (`id`, `content`) VALUES (1, 'c1')");
        }

        connection.setAutoCommit(false);
        try (Statement st = connection.createStatement()) {
            st.execute("START TRANSACTION");
            // Acquire a shared lock on id=1 so that the sink update hits the lock wait timeout.
            st.execute("SELECT * FROM " + tableName + " WHERE id=1 LOCK IN SHARE MODE;");

            final Map<String, String> properties = getDefaultSinkConfig();
            properties.put(JdbcSinkConnectorConfig.ENABLE_SHARED_CHANGE_EVENT_SINK_FIELD.name(), "true");
            properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
            properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
            properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
            properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
            properties.put(JdbcSinkConnectorConfig.CONNECTION_URL, getSink().getJdbcUrl(Map.of("sessionVariables", "innodb_lock_wait_timeout=5")));
            properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, tableName);
            properties.put(JdbcSinkConnectorConfig.FLUSH_MAX_RETRIES, "1");
            properties.put(JdbcSinkConnectorConfig.FLUSH_RETRY_DELAY_MS, "1000");
            startSinkConnector(properties);
            assertSinkConnectorIsRunning();

            final String topicName = topicName("server1", "schema", tableName);
            final JdbcSinkConnectorConfig config = getConfig(properties);
            final JdbcKafkaSinkRecord updateRecord = factory.updateRecordWithSchemaValue(
                    topicName, (byte) 1, "content", Schema.OPTIONAL_STRING_SCHEMA, "c11", config);

            // A transient (lock wait timeout) failure must fail the task after retries are
            // exhausted; it must never be routed to the errant record reporter.
            assertThatThrownBy(() -> {
                consume(updateRecord);
                consume(List.of());
            }).isInstanceOf(RuntimeException.class);

            assertThat(reportedRecords).isEmpty();
        }
        finally {
            connection.close();
            stopSinkConnector();
        }
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
