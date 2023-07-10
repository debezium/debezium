/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.mysql;

import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkInsertModeTest;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.MySqlSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Insert Mode tests for MySQL.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-mysql")
@ExtendWith(MySqlSinkDatabaseContextProvider.class)
public class JdbcSinkInsertModeIT extends AbstractJdbcSinkInsertModeTest {

    public JdbcSinkInsertModeIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6637")
    public void testInsertModeInsertWithPrimaryKeyModeComplexRecordValue(SinkRecordFactory factory) {

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.INSERT.getValue());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        Schema geometrySchema = buildGeoTypeSchema("Geometry");

        Struct geometryValue = new Struct(geometrySchema)
                .put("wkb", "AQMAAAABAAAABQAAAAAAAAAAAAAAAAAAAAAAFEAAAAAAAAAAQAAAAAAAABRAAAAAAAAAAEAAAAAAAAAcQAAAAAAAAAAAAAAAAAAAHEAAAAAAAAAAAAAAAAAAABRA".getBytes())
                .put("srid", 3187);

        Schema pointSchema = buildGeoTypeSchema("Point");
        Struct pointValue = new Struct(pointSchema)
                .put("x", 1.0)
                .put("y", 1.0)
                .put("wkb", "AQEAAAAAAAAAAADwPwAAAAAAAPA/".getBytes())
                .put("srid", 3187);

        final SinkRecord createGeometryRecord = factory.createRecordWithSchemaValue(topicName, (byte) 1,
                List.of("geometry", "point"), List.of(geometrySchema, pointSchema), List.of(geometryValue, pointValue));
        consume(createGeometryRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createGeometryRecord));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);

        // ST_GeomFromText('POLYGON ((0 5, 2 5, 2 7, 0 7, 0 5))', 3187)
        getSink().assertColumnType(tableAssert, "geometry", ValueType.BYTES, DatatypeConverter
                .parseHexBinary(
                        "730C000001030000000100000005000000000000000000000000000000000014400000000000000040000000000000144000000000000000400000000000001C4000000000000000000000000000001C4000000000000000000000000000001440"));

        // getSink().assertColumn(destinationTableName(createGeometryRecord), "geometry", "\"postgis\".\"geometry\"");

        // ST_PointFromText('POINT (1 1)', 3187)
        getSink().assertColumnType(tableAssert, "point", ValueType.BYTES, DatatypeConverter
                .parseHexBinary(
                        "730C00000101000000000000000000F03F000000000000F03F"));
    }

    private static Schema buildGeoTypeSchema(String type) {

        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .name("io.debezium.data.geometry." + type)
                .field("wkb", Schema.BYTES_SCHEMA)
                .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
                .optional();
        if ("Point".equals(type)) {
            schemaBuilder
                    .field("x", Schema.FLOAT64_SCHEMA)
                    .field("y", Schema.FLOAT64_SCHEMA);
        }
        return schemaBuilder
                .build();
    }
}
