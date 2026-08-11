/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

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
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * UNNEST batch write tests for PostgreSQL with array columns.
 *
 * <p>The UNNEST path binds one array per column carrying the value of every row in the batch, which an
 * array column cannot express. Records with an ARRAY field must therefore fall back to the row-wise
 * path, which this test verifies end to end.</p>
 *
 * @author Debezium Authors
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkUnnestArrayIT extends AbstractJdbcSinkTest {

    public JdbcSinkUnnestArrayIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2399")
    public void testUnnestBatchWithArrayColumn(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "true");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);
        final Schema arraySchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();

        final JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord record1 = factory.createRecordWithSchemaValue(
                topicName, (byte) 1, "col_array", arraySchema, List.of("a", "b"), config);
        final JdbcKafkaSinkRecord record2 = factory.createRecordWithSchemaValue(
                topicName, (byte) 2, "col_array", arraySchema, List.of("c"), config);

        // A single call with more than one record produces one multi-record batch, which is the
        // precondition for the UNNEST statement (a single record takes the per-row path).
        consume(List.of(record1, record2));

        getSink().assertRows(destinationTableName(record1), rs -> {
            assertThat(rs.getArray("col_array").getArray()).isEqualTo(new String[]{ "a", "b" });
            assertThat(rs.next()).isTrue();
            assertThat(rs.getArray("col_array").getArray()).isEqualTo(new String[]{ "c" });
            return null;
        });
    }
}
