/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.singlestore;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.SingleStoreSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.data.Json;

/**
 * JSON type mappings for SingleStore.
 */
@Tag("all")
@Tag("it")
@Tag("it-singlestore")
@ExtendWith(SingleStoreSinkDatabaseContextProvider.class)
public class JdbcSinkJsonTypeMappingIT extends AbstractJdbcSinkTest {

    public JdbcSinkJsonTypeMappingIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    public void testShouldCreateAndWriteJsonAndMapColumnTypes(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);
        final Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("json_data", "map_data"),
                List.of(Json.schema(), mapSchema),
                List.of("{\"a\":1}", Map.of("b", "2")),
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertColumn(destinationTable, "json_data", "JSON");
        getSink().assertColumn(destinationTable, "map_data", "JSON");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getString("json_data")).contains("\"a\"");
            assertThat(rs.getString("map_data")).contains("\"b\"");
            return null;
        });
    }
}
