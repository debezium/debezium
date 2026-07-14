/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.config.ConfigurationNames;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.metrics.JdbcSinkConnectorMetricsMXBean;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Integration tests verifying the JDBC sink connector's JMX metrics.
 *
 */
public abstract class AbstractJdbcSinkMetricsTest extends AbstractJdbcSinkTest {

    private static final String CONNECTOR_NAME = "jdbc-sink-metrics-it";
    private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    public AbstractJdbcSinkMetricsTest(Sink sink) {
        super(sink);
    }

    private ObjectName sinkMetricsObjectName() throws Exception {
        return new ObjectName("debezium.jdbc:type=connector-metrics,context=sink,server=" + CONNECTOR_NAME + ",task=0");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7261")
    public void testSinkMetricsCountUpsertModeAndDelete(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(ConfigurationNames.CONNECTOR_NAME_PROPERTY, CONNECTOR_NAME);
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        if ("true".equals(properties.get(JdbcSinkConnectorConfig.ENABLE_SHARED_CHANGE_EVENT_SINK_FIELD.name()))) {
            properties.put(JdbcSinkConnectorConfig.KEYED_MESSAGE_BATCH_MODE, SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH.getValue());
        }
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final ObjectName objectName = sinkMetricsObjectName();
        assertThat(MBEAN_SERVER.isRegistered(objectName)).isTrue();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        // two upsert-mode writes (insert + update of the same key) and one delete
        consume(factory.createRecord(topicName, (byte) 1, config));
        consume(factory.updateRecord(topicName, config));
        consume(factory.deleteRecord(topicName, config));

        assertThat(MBEAN_SERVER.getAttribute(objectName, JdbcSinkConnectorMetricsMXBean.METRIC_TOTAL_NUMBER_OF_WRITES)).isEqualTo(2L);
        assertThat(MBEAN_SERVER.getAttribute(objectName, JdbcSinkConnectorMetricsMXBean.METRIC_TOTAL_NUMBER_OF_DELETES)).isEqualTo(1L);
        assertThat(MBEAN_SERVER.getAttribute(objectName, JdbcSinkConnectorMetricsMXBean.METRIC_TOTAL_NUMBER_OF_TRUNCATES)).isEqualTo(0L);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7261")
    public void testSinkMetricsCountSchemaChanges(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(ConfigurationNames.CONNECTOR_NAME_PROPERTY, CONNECTOR_NAME);
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final ObjectName objectName = sinkMetricsObjectName();
        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        // First record for a table that does not exist yet: the sink creates it
        consume(factory.createRecord(topicName, (byte) 1, config));
        assertThat(MBEAN_SERVER.getAttribute(objectName, JdbcSinkConnectorMetricsMXBean.METRIC_TOTAL_NUMBER_OF_TABLES_CREATED)).isEqualTo(1L);
        assertThat(MBEAN_SERVER.getAttribute(objectName, JdbcSinkConnectorMetricsMXBean.METRIC_TOTAL_NUMBER_OF_TABLES_ALTERED)).isEqualTo(0L);

        // Record carrying a new optional field: the sink alters the table to add the column
        consume(factory.updateRecordWithSchemaValue(topicName, (byte) 1, "extra", Schema.OPTIONAL_STRING_SCHEMA, "v1", config));
        assertThat(MBEAN_SERVER.getAttribute(objectName, JdbcSinkConnectorMetricsMXBean.METRIC_TOTAL_NUMBER_OF_TABLES_CREATED)).isEqualTo(1L);
        assertThat(MBEAN_SERVER.getAttribute(objectName, JdbcSinkConnectorMetricsMXBean.METRIC_TOTAL_NUMBER_OF_TABLES_ALTERED)).isEqualTo(1L);

        // Record whose columns already exist: no schema change is applied and the counters stay put
        consume(factory.createRecord(topicName, (byte) 2, config));
        assertThat(MBEAN_SERVER.getAttribute(objectName, JdbcSinkConnectorMetricsMXBean.METRIC_TOTAL_NUMBER_OF_TABLES_CREATED)).isEqualTo(1L);
        assertThat(MBEAN_SERVER.getAttribute(objectName, JdbcSinkConnectorMetricsMXBean.METRIC_TOTAL_NUMBER_OF_TABLES_ALTERED)).isEqualTo(1L);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7261")
    public void testSinkMetricsUnregisteredOnStop(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(ConfigurationNames.CONNECTOR_NAME_PROPERTY, CONNECTOR_NAME);
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final ObjectName objectName = sinkMetricsObjectName();
        assertThat(MBEAN_SERVER.isRegistered(objectName)).isTrue();

        stopSinkConnector();

        assertThat(MBEAN_SERVER.isRegistered(objectName)).isFalse();
    }
}
