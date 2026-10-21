/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.singlestore;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
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
import io.debezium.connector.jdbc.junit.jupiter.SingleStoreSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.doc.FixFor;

import mil.nga.wkb.geom.LineString;
import mil.nga.wkb.geom.Polygon;
import mil.nga.wkb.io.ByteWriter;
import mil.nga.wkb.io.WkbGeometryWriter;

/**
 * Column type mappings for SingleStore.
 */
@Tag("all")
@Tag("it")
@Tag("it-singlestore")
@ExtendWith(SingleStoreSinkDatabaseContextProvider.class)
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

        final ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put((byte) 1);
        buffer.put((byte) 2);
        buffer.put((byte) 3);

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_BYTES_SCHEMA,
                buffer,
                config);

        final String destinationTable = destinationTableName(createRecord);
        getSink().execute(String.format("CREATE TABLE %s (id int not null, data binary(3), primary key(id))", destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getBytes(2)).isEqualTo(new byte[]{ 1, 2, 3 });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-2146")
    public void testShouldCreateAndWriteGeospatialColumnTypes(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);
        final byte[] pointWkb = Base64.getDecoder().decode("AQEAAAAAAAAAAADwPwAAAAAAAPA/");
        final byte[] polygonWkb = Base64.getDecoder().decode(
                "AQMAAAABAAAABQAAAAAAAAAAAAAAAAAAAAAAFEAAAAAAAAAAQAAAAAAAABRAAAAAAAAAAEAAAAAAAAAcQAAAAAAAAAAAAAAAAAAAHEAAAAAAAAAAAAAAAAAAABRA");
        final Schema pointSchema = Point.builder().build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("geometry_data", "point_data"),
                List.of(Geometry.schema(), pointSchema),
                List.of(
                        Geometry.createValue(Geometry.schema(), polygonWkb, 4326),
                        Point.createValue(pointSchema, pointWkb, 4326)),
                config);

        final String destinationTable = destinationTableName(createRecord);

        // SingleStore supports GEOGRAPHY/GEOGRAPHYPOINT columns only in rowstore tables, so create the
        // table explicitly with schema evolution disabled rather than letting the connector emit a
        // (columnstore by default) CREATE TABLE that SingleStore would reject.
        getSink().execute(String.format(
                "CREATE ROWSTORE TABLE %s (id tinyint NOT NULL, geometry_data geography, point_data geographypoint NOT NULL, PRIMARY KEY(id))",
                destinationTable));

        consume(createRecord);

        getSink().assertColumn(destinationTable, "geometry_data", "GEOGRAPHY");
        getSink().assertColumn(destinationTable, "point_data", "GEOGRAPHYPOINT");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getObject("geometry_data")).isNotNull();
            assertThat(rs.getObject("point_data")).isNotNull();
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-2146")
    public void testShouldCreateAndWriteVectorColumnTypes(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);
        final Schema floatVectorSchema = FloatVector.builder()
                .parameter("__debezium.source.column.length", "3")
                .build();
        final Schema doubleVectorSchema = DoubleVector.builder()
                .parameter("__debezium.source.column.length", "3")
                .build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("float_vector_data", "double_vector_data"),
                List.of(floatVectorSchema, doubleVectorSchema),
                List.of(List.of(1.0f, 2.0f, 3.0f), List.of(1.0d, 2.0d, 3.0d)),
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        // With the default SingleStore JDBC driver options, VECTOR columns are surfaced through JDBC
        // metadata as VARCHAR. Enabling the driver's extended data types (enableExtendedDataTypes=true)
        // would report them as VECTOR instead; this test exercises the default options, and the row
        // assertions below confirm the values still round-trip as an actual vector.
        getSink().assertColumn(destinationTable, "float_vector_data", "VARCHAR");
        getSink().assertColumn(destinationTable, "double_vector_data", "VARCHAR");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(normalizeVector(rs.getString("float_vector_data"))).isEqualTo("[1,2,3]");
            assertThat(normalizeVector(rs.getString("double_vector_data"))).isEqualTo("[1,2,3]");
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-2880")
    public void testShouldWriteMultiPolygon(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        // Two independent polygons: the first has one hole, and the second has two holes.
        final byte[] wkb = Base64.getDecoder().decode(
                "AQYAAAACAAAAAQMAAAACAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQQAAAAAAAAAAAAAAAAAAAEEAAAAAAAAAQQAAAAAAAAAAAAAAAAAAAEEAAAAAA" +
                        "AAAAAAAAAAAAAAAABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAA8D8AAAAAAADwPwAA" +
                        "AAAAAPA/AQMAAAADAAAABQAAAAAAAAAAABhAAAAAAAAAAAAAAAAAAAAoQAAAAAAAAAAAAAAAAAAAKEAAAAAAAAAQQAAAAAAAABhAAAAAAAAAEEAAAAAAAAAY" +
                        "QAAAAAAAAAAABQAAAAAAAAAAABxAAAAAAAAA8D8AAAAAAAAcQAAAAAAAAABAAAAAAAAAIEAAAAAAAAAAQAAAAAAAACBAAAAAAAAA8D8AAAAAAAAcQAAAAAAA" +
                        "APA/BQAAAAAAAAAAACJAAAAAAAAAAEAAAAAAAAAiQAAAAAAAAAhAAAAAAAAAJEAAAAAAAAAIQAAAAAAAACRAAAAAAAAAAEAAAAAAAAAiQAAAAAAAAABA");
        final JdbcKafkaSinkRecord record = factory.createRecordWithSchemaValue(
                topicName("server2", "schema", randomTableName()), (byte) 1, "geometry_data", Geometry.schema(),
                Geometry.createValue(Geometry.schema(), wkb, 4326), new JdbcSinkConnectorConfig(properties));
        final String destinationTable = destinationTableName(record);
        getSink().execute(String.format(
                "CREATE ROWSTORE TABLE %s (id tinyint NOT NULL, geometry_data geography, PRIMARY KEY(id))", destinationTable));
        consume(record);
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getObject("geometry_data")).isNotNull();
            try (var statement = rs.getStatement().getConnection().createStatement();
                    var checks = statement.executeQuery("SELECT GEOGRAPHY_CONTAINS(geometry_data, 'POINT(3 3)'), " +
                            "GEOGRAPHY_CONTAINS(geometry_data, 'POINT(11 1)'), " +
                            "GEOGRAPHY_CONTAINS(geometry_data, 'POINT(1.5 1.5)'), " +
                            "GEOGRAPHY_CONTAINS(geometry_data, 'POINT(7.5 1.5)'), " +
                            "GEOGRAPHY_CONTAINS(geometry_data, 'POINT(9.5 2.5)') FROM " + destinationTable)) {
                assertThat(checks.next()).isTrue();
                assertThat(checks.getBoolean(1)).isTrue();
                assertThat(checks.getBoolean(2)).isTrue();
                assertThat(checks.getBoolean(3)).isFalse();
                assertThat(checks.getBoolean(4)).isFalse();
                assertThat(checks.getBoolean(5)).isFalse();
            }
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2909")
    public void testShouldRepairPolygonAfterPrecisionRounding(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.SINGLESTORE_GEOMETRY_REPAIR_ENABLED, "true");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final var polygon = new Polygon(false, false);
        final var ring = new LineString(false, false);
        for (double[] coordinate : new double[][]{
                { 127.0, 37.0 }, { 127.1, 37.0 }, { 127.1, 37.1 },
                { 127.050000004, 37.1 }, { 127.050000004, 37.05 },
                { 127.050000001, 37.05 }, { 127.050000001, 37.1 },
                { 127.0, 37.1 }, { 127.0, 37.0 } }) {
            ring.addPoint(new mil.nga.wkb.geom.Point(coordinate[0], coordinate[1]));
        }
        polygon.addRing(ring);
        final var writer = new ByteWriter();
        writer.setByteOrder(ByteOrder.LITTLE_ENDIAN);
        WkbGeometryWriter.writeGeometry(writer, polygon);

        final JdbcKafkaSinkRecord record = factory.createRecordWithSchemaValue(
                topicName("server2", "schema", randomTableName()), (byte) 1, "geometry_data", Geometry.schema(),
                Geometry.createValue(Geometry.schema(), writer.getBytes(), 4326), new JdbcSinkConnectorConfig(properties));
        final String destinationTable = destinationTableName(record);
        getSink().execute(String.format("CREATE ROWSTORE TABLE %s (id tinyint NOT NULL, geometry_data geography, PRIMARY KEY(id))", destinationTable));
        consume(record);
        getSink().assertRows(destinationTable, rs -> {
            try (var statement = rs.getStatement().getConnection().createStatement();
                    var checks = statement.executeQuery("SELECT GEOGRAPHY_CONTAINS(geometry_data, 'POINT(127.025 37.05)') " +
                            "FROM " + destinationTable)) {
                assertThat(checks.next()).isTrue();
                assertThat(checks.getBoolean(1)).isTrue();
            }
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2909")
    public void testShouldPreserveAdjacentRoundedVerticesAfterInsert(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.SINGLESTORE_GEOMETRY_REPAIR_ENABLED, "true");
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final var polygon = new Polygon(false, false);
        final var ring = new LineString(false, false);
        for (double[] coordinate : new double[][]{
                { 127.0, 37.0 }, { 127.1, 37.0 }, { 127.1, 37.1 },
                { 127.050000024, 37.1 }, { 127.050000014, 37.1 },
                { 127.0, 37.1 }, { 127.0, 37.0 } }) {
            ring.addPoint(new mil.nga.wkb.geom.Point(coordinate[0], coordinate[1]));
        }
        polygon.addRing(ring);
        final var writer = new ByteWriter();
        writer.setByteOrder(ByteOrder.LITTLE_ENDIAN);
        WkbGeometryWriter.writeGeometry(writer, polygon);

        final JdbcKafkaSinkRecord record = factory.createRecordWithSchemaValue(
                topicName("server2", "schema", randomTableName()), (byte) 1, "geometry_data", Geometry.schema(),
                Geometry.createValue(Geometry.schema(), writer.getBytes(), 4326), new JdbcSinkConnectorConfig(properties));
        final String destinationTable = destinationTableName(record);
        getSink().execute(String.format("CREATE ROWSTORE TABLE %s (id tinyint NOT NULL, geometry_data geography, PRIMARY KEY(id))", destinationTable));
        consume(record);
        getSink().assertRows(destinationTable, rs -> {
            final String storedWkt = rs.getString("geometry_data");
            assertThat(storedWkt).contains("127.05000001 ", "127.05000002 ");
            return null;
        });
    }

    private static String normalizeVector(String value) {
        if (value == null) {
            return null;
        }
        return value.replace(" ", "").replace(".0", "");
    }
}
