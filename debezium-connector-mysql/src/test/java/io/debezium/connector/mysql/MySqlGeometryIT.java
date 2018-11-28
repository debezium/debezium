/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;

import javax.xml.bind.DatatypeConverter;

import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;
import mil.nga.wkb.geom.Point;
import mil.nga.wkb.io.ByteReader;
import mil.nga.wkb.io.WkbGeometryReader;

/**
 * @author Omar Al-Safi
 */
public class MySqlGeometryIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-json.txt")
                                                             .toAbsolutePath();
    private UniqueDatabase DATABASE;
    private DatabaseDifferences databaseAsserts;

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        databaseAsserts = MySQLConnection.forTestDatabase("emptydb").databaseAsserts();
        DATABASE = new UniqueDatabase("geometryit", databaseAsserts.geometryDatabaseName())
                .withDbHistoryPath(DB_HISTORY_PATH);
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        //Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 2;
        int numDataRecords = databaseAsserts.geometryPointTableRecords() + 2;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_222_point")).size()).isEqualTo(databaseAsserts.geometryPointTableRecords());
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_507_geometry")).size()).isEqualTo(2);
        assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(
            numCreateDatabase + numCreateTables);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("json_test")).isNull();
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_222_point")) {
                assertPoint(value);
            } else if (record.topic().endsWith("dbz_507_geometry")) {
                assertGeomRecord(value);
            }
        });
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig().build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        //Testing.Debug.enable();
        int numTables = 2;
        int numDataRecords = databaseAsserts.geometryPointTableRecords() + 2;
        int numDdlRecords =
            numTables * 2 + 3; // for each table (1 drop + 1 create) + for each db (1 create + 1 drop + 1 use)
        int numSetVariables = 1;
        SourceRecords records = consumeRecordsByTopic(numDdlRecords + numSetVariables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numDdlRecords + numSetVariables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_222_point")).size()).isEqualTo(databaseAsserts.geometryPointTableRecords());
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_507_geometry")).size()).isEqualTo(2);
        assertThat(records.topics().size()).isEqualTo(numTables + 1);
        assertThat(records.databaseNames()).containsOnly(DATABASE.getDatabaseName(), "");
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(numDdlRecords);
        assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("json_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1); // SET statement
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_222_point")) {
                assertPoint(value);
            } else if (record.topic().endsWith("dbz_507_geometry")) {
                assertGeomRecord(value);
            }
        });
    }

    private void assertPoint(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        Testing.debug(after);
        assertThat(i).isNotNull();
        Double expectedX = after.getFloat64("expected_x");
        Double expectedY = after.getFloat64("expected_y");
        Integer expectedSrid = after.getInt32("expected_srid");

        if (after.getStruct("point") != null) {
            Double actualX = after.getStruct("point").getFloat64("x");
            Double actualY = after.getStruct("point").getFloat64("y");
            Integer actualSrid = after.getStruct("point").getInt32("srid");
            //Validate the values
            databaseAsserts.geometryAssertPoints(expectedX, expectedY, actualX, actualY);
            assertThat(actualSrid).isEqualTo(expectedSrid);
            //Test WKB
            Point point = (Point) WkbGeometryReader.readGeometry(new ByteReader((byte[]) after.getStruct("point")
                    .get("wkb")));
            databaseAsserts.geometryAssertPoints(expectedX, expectedY, point.getX(), point.getY());
        } else if (expectedX != null) {
            Assert.fail("Got a null geometry but didn't expect to");
        }
    }

    private void assertGeomRecord(Struct value) {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Integer i = after.getInt32("id");
        Testing.debug(after);
        assertThat(i).isNotNull();
        if (i == 1) {
            // INSERT INTO dbz_507_geometry VALUES (1, ST_GeomFromText('POINT(1 1)', 4326), ST_GeomFromText('LINESTRING(0 0, 1 1)', 3187), ST_GeomFromText('POLYGON((0 0, 1 1, 1 0, 0 0))'), ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1))', 4326));
            assertThat(after.getStruct("geom").getInt32("srid")).isEqualTo(4326);
            assertThat(DatatypeConverter.printHexBinary(after.getStruct("geom").getBytes("wkb"))).isEqualTo("0101000000000000000000F03F000000000000F03F");

            assertThat(after.getStruct("linestring").getInt32("srid")).isEqualTo(3187);
            assertThat(DatatypeConverter.printHexBinary(after.getStruct("linestring").getBytes("wkb"))).isEqualTo("01020000000200000000000000000000000000000000000000000000000000F03F000000000000F03F");

            assertThat(after.getStruct("polygon").getInt32("srid")).isEqualTo(null);
            assertThat(DatatypeConverter.printHexBinary(after.getStruct("polygon").getBytes("wkb"))).isEqualTo("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000");

            assertThat(after.getStruct("collection").getInt32("srid")).isEqualTo(4326);
            assertThat(DatatypeConverter.printHexBinary(after.getStruct("collection").getBytes("wkb"))).isEqualTo("0107000000020000000101000000000000000000F03F000000000000F03F01020000000200000000000000000000000000000000000000000000000000F03F000000000000F03F");
        } else if (i == 2) {
            // INSERT INTO dbz_507_geometry VALUES (2, ST_GeomFromText('LINESTRING(0 0, 1 1)'), NULL, NULL, NULL);
            assertThat(after.getStruct("geom").getInt32("srid")).isEqualTo(null);
            assertThat(DatatypeConverter.printHexBinary(after.getStruct("geom").getBytes("wkb"))).isEqualTo("01020000000200000000000000000000000000000000000000000000000000F03F000000000000F03F");

            assertThat(after.getStruct("linestring")).isNull();
            assertThat(after.getStruct("polygon")).isNull();
            assertThat(after.getStruct("collection")).isNull();
        }

    }
}
