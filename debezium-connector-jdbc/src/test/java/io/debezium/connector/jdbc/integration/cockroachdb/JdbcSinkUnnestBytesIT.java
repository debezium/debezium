/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.cockroachdb;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.CockroachDbSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * UNNEST batch write tests for CockroachDB with binary columns.
 *
 * <p>The UNNEST path binds each column with {@code Connection#createArrayOf}, which the PostgreSQL
 * driver cannot serve for {@code bytea} elements; on CockroachDB the failure surfaced even earlier
 * because Hibernate's CockroachDB dialect names the binary DDL type {@code bytes}, an alias absent
 * from {@code pg_type}. Records with a BYTES field must therefore fall back to the row-wise path,
 * which this test verifies end to end against CockroachDB.</p>
 *
 * @author Virag Tripathi
 */
@Tag("all")
@Tag("it")
@Tag("it-cockroachdb")
@ExtendWith(CockroachDbSinkDatabaseContextProvider.class)
public class JdbcSinkUnnestBytesIT extends AbstractJdbcSinkTest {

    public JdbcSinkUnnestBytesIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2357")
    public void testUnnestBatchWithBytesColumn(SinkRecordFactory factory) {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.USE_REDUCTION_BUFFER, "true");
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, "true");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord record1 = factory.createRecordWithSchemaValue(
                topicName, (byte) 1, "col_bytes", Schema.OPTIONAL_BYTES_SCHEMA, new byte[]{ 0x01, 0x02 }, config);
        final JdbcKafkaSinkRecord record2 = factory.createRecordWithSchemaValue(
                topicName, (byte) 2, "col_bytes", Schema.OPTIONAL_BYTES_SCHEMA, new byte[]{ 0x03, 0x04 }, config);

        // A single call with more than one record produces one multi-record batch, which is the
        // precondition for the UNNEST statement (a single record takes the per-row path).
        consume(List.of(record1, record2));

        TestHelper.assertTable(assertDbConnection(), destinationTableName(record1))
                .hasNumberOfRows(2)
                .column("col_bytes").hasValues(new byte[]{ 0x01, 0x02 }, new byte[]{ 0x03, 0x04 });
    }
}
