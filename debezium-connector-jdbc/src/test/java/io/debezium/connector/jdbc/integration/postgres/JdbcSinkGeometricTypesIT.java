/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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
import io.debezium.data.geometry.Circle;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Line;
import io.debezium.data.geometry.Point;
import io.debezium.doc.FixFor;
import io.debezium.spatial.WkbWriter;

/**
 * Round-trip tests that PostgreSQL geometric types bind to their native column types on a plain
 * (non-PostGIS) PostgreSQL sink: the six types added in dbz#2135 (box/circle/line/lseg/path/polygon)
 * and the built-in point (dbz#2100, case 9). The target table is pre-created with native column types,
 * and the connector runs with source-type propagation so the sink can reconstruct the native values
 * for box/lseg/path/polygon.
 *
 * @author Debezium Authors
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkGeometricTypesIT extends AbstractJdbcSinkTest {

    public JdbcSinkGeometricTypesIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2135")
    public void testBoxRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Schema schema = geometrySchema("box");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildPolygon(List.of(List.of(
                        new double[]{ 1.0, 1.0 }, new double[]{ 1.0, 0.0 }, new double[]{ 0.0, 0.0 },
                        new double[]{ 0.0, 1.0 }, new double[]{ 1.0, 1.0 }))),
                null, Map.of(Geometry.EXTENSION_TYPE_KEY, "box"));

        assertRoundTrip(factory, insertMode, "box", schema, value, rs -> assertThat(rs.getString(2)).isEqualTo("(1,1),(0,0)"));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2135")
    public void testLsegRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Schema schema = geometrySchema("lseg");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildLineString(List.of(new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 1.0 })),
                null, Map.of(Geometry.EXTENSION_TYPE_KEY, "lseg"));

        assertRoundTrip(factory, insertMode, "lseg", schema, value, rs -> assertThat(rs.getString(2)).isEqualTo("[(0,0),(0,1)]"));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2135")
    public void testClosedPathRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Schema schema = geometrySchema("path");
        final Map<String, String> extensions = new LinkedHashMap<>();
        extensions.put(Geometry.EXTENSION_TYPE_KEY, "path");
        extensions.put(Geometry.EXTENSION_CLOSED_KEY, "true");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildLineString(List.of(
                        new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 1.0 }, new double[]{ 0.0, 2.0 })),
                null, extensions);

        assertRoundTrip(factory, insertMode, "path", schema, value, rs -> assertThat(rs.getString(2)).isEqualTo("((0,0),(0,1),(0,2))"));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2135")
    public void testPolygonRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Schema schema = geometrySchema("polygon");
        final Struct value = Geometry.createValue(schema,
                WkbWriter.buildPolygon(List.of(List.of(
                        new double[]{ 0.0, 0.0 }, new double[]{ 0.0, 1.0 }, new double[]{ 1.0, 0.0 },
                        new double[]{ 0.0, 0.0 }))),
                null, Map.of(Geometry.EXTENSION_TYPE_KEY, "polygon"));

        assertRoundTrip(factory, insertMode, "polygon", schema, value, rs -> assertThat(rs.getString(2)).isEqualTo("((0,0),(0,1),(1,0))"));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2135")
    public void testCircleRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Schema schema = Circle.schema();
        final Struct value = Circle.createValue(schema, 10.0, 4.0, 10.0);

        assertRoundTrip(factory, insertMode, "circle", schema, value, rs -> assertThat(rs.getString(2)).isEqualTo("<(10,4),10>"));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2135")
    public void testLineRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Schema schema = Line.schema();
        final Struct value = Line.createValue(schema, -1.0, 0.0, 0.0);

        assertRoundTrip(factory, insertMode, "line", schema, value, rs -> assertThat(rs.getString(2)).isEqualTo("{-1,0,0}"));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testPointRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Schema schema = Point.builder().build();
        final Struct value = Point.createValue(schema, 1.5, -2.0);

        assertRoundTrip(factory, insertMode, "point", schema, value, rs -> assertThat(rs.getString(2)).isEqualTo("(1.5,-2)"));
    }

    /**
     * A batch of more than one record is what triggers PostgreSQL's UNNEST path (a single record
     * short-circuits to a row-wise INSERT). Since the native geometric types reconstruct their value in
     * {@code JdbcType#bind()}, which the UNNEST array-binding bypasses, this asserts the sink still routes
     * box values through the row-wise path and reconstructs each of them correctly. Guards dbz#2135.
     */
    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2135")
    public void testBoxBatchRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Schema schema = geometrySchema("box");
        final List<String> expected = List.of("(1,1),(0,0)", "(3,3),(2,2)");

        final List<Struct> values = List.of(
                Geometry.createValue(schema,
                        WkbWriter.buildPolygon(List.of(List.of(
                                new double[]{ 1.0, 1.0 }, new double[]{ 1.0, 0.0 }, new double[]{ 0.0, 0.0 },
                                new double[]{ 0.0, 1.0 }, new double[]{ 1.0, 1.0 }))),
                        null, Map.of(Geometry.EXTENSION_TYPE_KEY, "box")),
                Geometry.createValue(schema,
                        WkbWriter.buildPolygon(List.of(List.of(
                                new double[]{ 3.0, 3.0 }, new double[]{ 3.0, 2.0 }, new double[]{ 2.0, 2.0 },
                                new double[]{ 2.0, 3.0 }, new double[]{ 3.0, 3.0 }))),
                        null, Map.of(Geometry.EXTENSION_TYPE_KEY, "box")));

        assertBatchRoundTrip(factory, insertMode, "box", schema, values, expected);
    }

    /**
     * The point counterpart to {@link #testBoxBatchRoundTrip}: a batch of more than one record is what
     * triggers PostgreSQL's UNNEST path, and since {@code point} is a {@code STRUCT} schema the sink must
     * fall back to the row-wise path (which reconstructs the {@code (x,y)} literal in {@code bind()})
     * rather than hand raw Structs to {@code createArrayOf}. Guards dbz#2100, case 9.
     */
    @ParameterizedTest
    @ArgumentsSource(PostgresInsertModeArgumentsProvider.class)
    @FixFor("debezium/dbz#2100")
    public void testPointBatchRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode) throws Exception {
        final Schema schema = Point.builder().build();
        final List<Struct> values = List.of(
                Point.createValue(schema, 1.5, -2.0),
                Point.createValue(schema, 3.0, 4.0));
        final List<String> expected = List.of("(1.5,-2)", "(3,4)");

        assertBatchRoundTrip(factory, insertMode, "point", schema, values, expected);
    }

    private void assertRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode, String columnType,
                                 Schema fieldSchema, Object value, ResultSetConsumer assertion)
            throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "false");
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String topicName = topicName("server1", "schema", randomTableName());

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(topicName, (byte) 1, "data", fieldSchema, value, config);

        final String destinationTable = destinationTableName(createRecord);
        getSink().execute(String.format("CREATE TABLE %s (id int not null, data %s null, primary key(id))", destinationTable, columnType));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertion.accept(rs);
            return null;
        });
    }

    private void assertBatchRoundTrip(SinkRecordFactory factory, PostgresInsertMode insertMode, String columnType,
                                      Schema fieldSchema, List<Struct> values, List<String> expectedTexts)
            throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "false");
        properties.put(JdbcSinkConnectorConfig.POSTGRES_UNNEST_INSERT, String.valueOf(insertMode.isUnnestEnabled()));
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String topicName = topicName("server1", "schema", randomTableName());
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        final List<JdbcKafkaSinkRecord> records = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            records.add(factory.createRecordWithSchemaValue(topicName, (byte) (i + 1), "data", fieldSchema, values.get(i), config));
        }

        final String destinationTable = destinationTableName(records.get(0));
        getSink().execute(String.format("CREATE TABLE %s (id int not null, data %s null, primary key(id))", destinationTable, columnType));

        consume(records);

        try (Statement st = getSink().getConnection().createStatement();
                ResultSet rs = st.executeQuery("SELECT id, data FROM " + destinationTable + " ORDER BY id")) {
            for (int i = 0; i < expectedTexts.size(); i++) {
                assertThat(rs.next()).as("row %d present", i + 1).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(i + 1);
                assertThat(rs.getString(2)).isEqualTo(expectedTexts.get(i));
            }
            assertThat(rs.next()).as("no extra rows").isFalse();
        }
    }

    /**
     * Builds the shared {@link Geometry} schema carrying the propagated native source-column type, which
     * is what drives the sink's native (non-PostGIS) binding branch.
     */
    private static Schema geometrySchema(String sourceType) {
        final SchemaBuilder builder = SchemaBuilder.struct()
                .name(Geometry.LOGICAL_NAME)
                .version(2)
                .optional()
                .parameter("__debezium.source.column.type", sourceType)
                .field(Geometry.WKB_FIELD, Schema.BYTES_SCHEMA)
                .field(Geometry.SRID_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
                .field(Geometry.EXTENSIONS_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build());
        return builder.build();
    }

    @FunctionalInterface
    private interface ResultSetConsumer {
        void accept(java.sql.ResultSet rs) throws Exception;
    }
}
