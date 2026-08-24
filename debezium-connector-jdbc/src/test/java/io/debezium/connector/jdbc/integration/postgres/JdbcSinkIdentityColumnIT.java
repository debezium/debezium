/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
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
import io.debezium.connector.jdbc.junit.jupiter.PostgresInsertModeArgumentsProvider;
import io.debezium.connector.jdbc.junit.jupiter.PostgresInsertModeArgumentsProvider.PostgresInsertMode;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Tests writing into a PostgreSQL table whose primary key is an identity column.
 *
 * @author Debezium Authors
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkIdentityColumnIT extends AbstractJdbcSinkTest {

    public JdbcSinkIdentityColumnIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2459")
    public void testUpsertIntoGeneratedAlwaysIdentityPrimaryKey(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        writeTwoRecords(factory, insertMode, JdbcSinkConnectorConfig.InsertMode.UPSERT,
                "CREATE TABLE %s (id int generated always as identity, data text, primary key(id))");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2459")
    public void testInsertIntoGeneratedAlwaysIdentityPrimaryKey(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        writeTwoRecords(factory, insertMode, JdbcSinkConnectorConfig.InsertMode.INSERT,
                "CREATE TABLE %s (id int generated always as identity, data text, primary key(id))");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2459")
    public void testUpsertIntoGeneratedAlwaysIdentityPrimaryKeyThatIsNotTheFirstColumn(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        writeTwoRecords(factory, insertMode, JdbcSinkConnectorConfig.InsertMode.UPSERT,
                "CREATE TABLE %s (data text, id int generated always as identity, primary key(id))");
    }

    /**
     * Writes two records in one batch into a table created by {@code createTableSql}, then asserts both
     * landed. Two records rather than one so that the UNNEST batch path is taken when it is enabled;
     * it falls back to the row-wise path for a single record.
     */
    private void writeTwoRecords(SinkRecordFactory factory, PostgresInsertMode insertMode, JdbcSinkConnectorConfig.InsertMode mode, String createTableSql)
            throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, mode.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String topicName = topicName("server1", "schema", randomTableName());

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(getConfig(properties));
        final JdbcKafkaSinkRecord first = factory.createRecordWithSchemaValue(
                topicName, (byte) 1, "data", Schema.OPTIONAL_STRING_SCHEMA, "a", config);
        final JdbcKafkaSinkRecord second = factory.createRecordWithSchemaValue(
                topicName, (byte) 2, "data", Schema.OPTIONAL_STRING_SCHEMA, "b", config);

        final String destinationTable = destinationTableName(first);
        getSink().execute(String.format(createTableSql, destinationTable));

        consume(List.of(first, second));

        getSink().assertRows(destinationTable, rs -> {
            final Map<Integer, String> rows = new HashMap<>();
            do {
                rows.put(rs.getInt("id"), rs.getString("data"));
            } while (rs.next());
            assertThat(rows).containsExactlyInAnyOrderEntriesOf(Map.of(1, "a", 2, "b"));
            return null;
        });
    }
}
