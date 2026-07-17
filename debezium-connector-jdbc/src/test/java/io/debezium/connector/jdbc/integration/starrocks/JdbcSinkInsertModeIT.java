/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.starrocks;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkInsertModeTest;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.junit.jupiter.StarRocksSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Insert mode tests for StarRocks.
 */
@Tag("all")
@Tag("it")
@Tag("it-starrocks")
@ExtendWith(StarRocksSinkDatabaseContextProvider.class)
public class JdbcSinkInsertModeIT extends AbstractJdbcSinkInsertModeTest {

    public JdbcSinkInsertModeIT(Sink sink) {
        super(sink);
    }

    @Override
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testInsertModeUpdateWithNoPrimaryKey(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPDATE.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        final JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord record = factory.createRecordNoKey(topicName, config);

        try {
            consume(record);
            // Legacy mode reports a failed put on the next call, while the shared sink
            // framework reports the failure during the first pre-commit flush.
            consume(record);
            fail("Expected StarRocks to reject UPDATE on a Duplicate Key table");
        }
        catch (Exception e) {
            assertExceptionCauseMessage(e, "table .* does not support update");
        }
        finally {
            stopSinkConnector();
        }
    }
}
