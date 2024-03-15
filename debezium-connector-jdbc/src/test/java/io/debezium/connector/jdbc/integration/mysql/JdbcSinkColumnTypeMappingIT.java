/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.MySqlSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Column type mappings for MySQL
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-mysql")
@ExtendWith(MySqlSinkDatabaseContextProvider.class)
public class JdbcSinkColumnTypeMappingIT extends AbstractJdbcSinkTest {

    public JdbcSinkColumnTypeMappingIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6967")
    public void testShouldCoerceNioByteBufferTypeToByteArrayColumnType(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put((byte) 1);
        buffer.put((byte) 2);
        buffer.put((byte) 3);

        final SinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_BYTES_SCHEMA,
                buffer);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data binary(3), primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getBytes(2)).isEqualTo(new byte[]{ 1, 2, 3 });
            return null;
        });
    }
}
