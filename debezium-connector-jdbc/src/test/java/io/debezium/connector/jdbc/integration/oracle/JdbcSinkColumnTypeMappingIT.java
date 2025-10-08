/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.OracleSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SdoGeometryUtil;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.data.geometry.Geometry;
import io.debezium.doc.FixFor;

/**
 * Column type mapping tests for Oracle.
 *
 * @author Andrey Pustovetov
 */
@Tag("all")
@Tag("it")
@Tag("it-oracle")
@ExtendWith(OracleSinkDatabaseContextProvider.class)
public class JdbcSinkColumnTypeMappingIT extends AbstractJdbcSinkTest {

    public JdbcSinkColumnTypeMappingIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7620")
    public void testShouldCoerceNioByteBufferTypeToByteArrayColumnType(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put((byte) 1);
        buffer.put((byte) 2);
        buffer.put((byte) 3);

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_BYTES_SCHEMA,
                buffer);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id number(9,0) not null, data blob, primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getBytes(2)).isEqualTo(new byte[]{ 1, 2, 3 });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-9508")
    public void testShouldCoerceGeometryTypeToSdoGeometryColumnType(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.INSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final Schema optionalGeometrySchema = Geometry.builder().optional().build();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final byte[] wkb = new byte[]{ 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 64, 0, 0, 0, 0, 0, 0, 76, 64 };

        final Struct geometry = Geometry.createValue(optionalGeometrySchema, wkb, 4326);

        final Object[] expectedValue = SdoGeometryUtil.toSdoGeometryAttributes(2001, 4326, new double[]{ 8, 56 });

        final KafkaDebeziumSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                optionalGeometrySchema,
                geometry);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id number(9,0) not null, data mdsys.sdo_geometry, primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getObject(2)).isInstanceOf(java.sql.Struct.class);
            Object[] value = SdoGeometryUtil.toSdoGeometryAttributes((java.sql.Struct) rs.getObject(2));
            assertThat(value).isEqualTo(expectedValue);
            return null;
        });
    }
}
