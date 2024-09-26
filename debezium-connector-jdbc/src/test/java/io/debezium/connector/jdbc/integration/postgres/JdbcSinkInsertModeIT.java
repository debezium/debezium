/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.DataSourceWithLetterCase;
import org.assertj.db.type.ValueType;
import org.assertj.db.type.lettercase.CaseComparisons;
import org.assertj.db.type.lettercase.CaseConversions;
import org.assertj.db.type.lettercase.LetterCase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.postgresql.geometric.PGpoint;
import org.postgresql.util.PGobject;

import io.debezium.connector.jdbc.FieldSchemaAndValue;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.PrimaryKeyMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkInsertModeTest;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.junit.jupiter.WithPostgresExtension;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.data.geometry.Geometry;
import io.debezium.doc.FixFor;

/**
 * Insert Mode tests for PostgreSQL.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkInsertModeIT extends AbstractJdbcSinkInsertModeTest {

    public static final LetterCase LOWER_CASE_STRICT = LetterCase.getLetterCase(CaseConversions.LOWER, CaseComparisons.STRICT);
    public static final LetterCase UPPER_CASE_STRICT = LetterCase.getLetterCase(CaseConversions.UPPER, CaseComparisons.STRICT);

    public JdbcSinkInsertModeIT(Sink sink) {
        super(sink);
    }

    @WithPostgresExtension("postgis")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6637")
    public void testInsertModeInsertWithPrimaryKeyModeComplexRecordValue(SinkRecordFactory factory) throws SQLException {

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_POSTGIS_SCHEMA, "postgis");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        Schema geometrySchema = buildGeoTypeSchema("Geometry");

        Struct geometryValue = new Struct(geometrySchema)
                .put("wkb", Base64.getDecoder().decode(
                        "AQMAAAABAAAABQAAAAAAAAAAAAAAAAAAAAAAFEAAAAAAAAAAQAAAAAAAABRAAAAAAAAAAEAAAAAAAAAcQAAAAAAAAAAAAAAAAAAAHEAAAAAAAAAAAAAAAAAAABRA".getBytes()));

        Schema pointSchema = buildGeoTypeSchema("Point");
        Struct pointValue = new Struct(pointSchema)
                .put("x", 1.0)
                .put("y", 1.0)
                .put("wkb", Base64.getDecoder().decode("AQEAAAAAAAAAAADwPwAAAAAAAPA/".getBytes()))
                .put("srid", 3187);

        Schema geographySchema = buildGeoTypeSchema("Geography");

        Struct geographyValue = new Struct(geographySchema)
                .put("wkb", Base64.getDecoder().decode("AQUAACDmEAAAAQAAAAECAAAAAgAAAKd5xyk6JGVAC0YldQJaRsDGbTSAt/xkQMPTK2UZUkbA".getBytes()))
                .put("srid", 4326);

        final SinkRecord createGeometryRecord = factory.createRecordWithSchemaValue(topicName, (byte) 1,
                List.of("geometry", "point", "geography", "p"), List.of(geometrySchema, pointSchema, geographySchema, pointSchema),
                Arrays.asList(new Object[]{ geometryValue, pointValue, geographyValue }));
        consume(createGeometryRecord);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(createGeometryRecord));
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(5);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1);

        // ST_GeomFromText('POLYGON ((0 5, 2 5, 2 7, 0 7, 0 5))', 3187)
        PGobject expectedValue = new PGobject();
        expectedValue.setType("\"postgis\".\"geometry\"");
        expectedValue.setValue(
                "01030000000100000005000000000000000000000000000000000014400000000000000040000000000000144000000000000000400000000000001C4000000000000000000000000000001C4000000000000000000000000000001440");
        getSink().assertColumnType(tableAssert, "geometry", PGobject.class, expectedValue);

        // ST_PointFromText('POINT (1 1)', 3187)
        PGpoint expectedPoint = new PGpoint(1.0, 1.0);
        getSink().assertColumnType(tableAssert, "point", PGobject.class, expectedPoint);

        // SRID=4326;MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))
        PGobject expectedGeographyValue = new PGobject();
        expectedGeographyValue.setType("\"postgis\".\"geography\"");
        expectedGeographyValue.setValue(
                "0105000020E610000001000000010200000002000000A779C7293A2465400B462575025A46C0C66D3480B7FC6440C3D32B65195246C0");
        getSink().assertColumnType(tableAssert, "geography", PGobject.class, expectedGeographyValue);

        getSink().assertColumnHasNullValue(tableAssert, "p");
    }

    @WithPostgresExtension("postgis")
    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-8221")
    public void testBatchWithDifferingSqlParameterBindings(SinkRecordFactory factory) throws SQLException {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.POSTGRES_POSTGIS_SCHEMA, "postgis");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord recordA = factory.createInsertSchemaAndValue(
                topicName,
                List.of(new FieldSchemaAndValue("id", Schema.STRING_SCHEMA, "12345")),
                List.of(
                        new FieldSchemaAndValue("gis_area",
                                Geometry.schema(),
                                Geometry.createValue(
                                        Geometry.schema(),
                                        Base64.getDecoder().decode("AQEAACARDWAAuooeV7P4V0EWN+bdvgBVQO==".getBytes()),
                                        3857)),
                        new FieldSchemaAndValue("__deleted", Schema.BOOLEAN_SCHEMA, false)),
                0);

        final SinkRecord recordB = factory.createInsertSchemaAndValue(
                topicName,
                List.of(new FieldSchemaAndValue("id", Schema.STRING_SCHEMA, "23456")),
                List.of(new FieldSchemaAndValue("gis_area", Geometry.schema(), null),
                        new FieldSchemaAndValue("__deleted", Schema.BOOLEAN_SCHEMA, false)),
                1);

        final SinkRecord recordC = factory.createInsertSchemaAndValue(
                topicName,
                List.of(new FieldSchemaAndValue("id", Schema.STRING_SCHEMA, "23456")),
                List.of(
                        new FieldSchemaAndValue("gis_area",
                                Geometry.schema(),
                                Geometry.createValue(
                                        Geometry.schema(),
                                        Base64.getDecoder().decode("AQEAACARDWAAuooeV7P4V0EWN+bdvgBVQO==".getBytes()),
                                        3857)),
                        new FieldSchemaAndValue("__deleted", Schema.BOOLEAN_SCHEMA, false)),
                0);

        final List<SinkRecord> records = List.of(recordA, recordB, recordC);
        consume(records);

        final TableAssert tableAssert = TestHelper.assertTable(dataSource(), destinationTableName(recordA));
        tableAssert.hasNumberOfRows(2).hasNumberOfColumns(3);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6682")
    public void testInsertModeInsertWithPrimaryKeyModeUpperCaseColumnNameWithQuotedIdentifiers(SinkRecordFactory factory) {

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "ID");
        properties.put(JdbcSinkConnectorConfig.QUOTE_IDENTIFIERS, "true");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createSimpleRecord1 = factory.createRecord(topicName, (byte) 1, String::toUpperCase);
        final SinkRecord createSimpleRecord2 = factory.createRecord(topicName, (byte) 2, String::toUpperCase);
        consume(createSimpleRecord1);
        consume(createSimpleRecord2);

        DataSourceWithLetterCase dataSourceWithLetterCase = new DataSourceWithLetterCase(dataSource(), LetterCase.TABLE_DEFAULT, UPPER_CASE_STRICT, UPPER_CASE_STRICT);
        final TableAssert tableAssert = TestHelper.assertTable(dataSourceWithLetterCase, destinationTableName(createSimpleRecord1));
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "ID", ValueType.NUMBER, (byte) 1, (byte) 2);
        getSink().assertColumnType(tableAssert, "NAME", ValueType.TEXT, "John Doe", "John Doe");
        getSink().assertColumnType(tableAssert, "NICK_NAME$", ValueType.TEXT, "John Doe$", "John Doe$");
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6682")
    public void testInsertModeInsertWithPrimaryKeyModeUpperCaseColumnNameWithoutQuotedIdentifiers(SinkRecordFactory factory) {

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "ID");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server1", "schema", tableName);

        final SinkRecord createSimpleRecord1 = factory.createRecord(topicName, (byte) 1, String::toUpperCase);
        final SinkRecord createSimpleRecord2 = factory.createRecord(topicName, (byte) 2, String::toUpperCase);
        consume(createSimpleRecord1);
        consume(createSimpleRecord2);

        DataSourceWithLetterCase dataSourceWithLetterCase = new DataSourceWithLetterCase(dataSource(), LetterCase.TABLE_DEFAULT, LOWER_CASE_STRICT, LOWER_CASE_STRICT);

        final TableAssert tableAssert = TestHelper.assertTable(dataSourceWithLetterCase, destinationTableName(createSimpleRecord1), null, null);
        tableAssert.exists().hasNumberOfRows(2).hasNumberOfColumns(3);

        getSink().assertColumnType(tableAssert, "id", ValueType.NUMBER, (byte) 1, (byte) 2);
        getSink().assertColumnType(tableAssert, "name", ValueType.TEXT, "John Doe", "John Doe");
        getSink().assertColumnType(tableAssert, "nick_name$", ValueType.TEXT, "John Doe$", "John Doe$");
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
