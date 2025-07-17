/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.vector.FloatVector;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * @author Jiri Pechanec
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 9, minor = 0, reason = "VECTOR datatype not added until MySQL 9.0")
public abstract class BinlogVectorIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private String databaseName;

    public BinlogVectorIT(final String databaseName){
        this.databaseName = databaseName;
    }

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-json.txt")
            .toAbsolutePath();
    protected UniqueDatabase DATABASE;

    protected Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();

        DATABASE = TestHelper.getUniqueDatabase("vectorit", databaseName)
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        DATABASE.createAndInitialize();

        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    /*
     * @Test
     * public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
     * // Use the DB configuration to define the connector's configuration ...
     * config = DATABASE.defaultConfig()
     * .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
     * .build();
     *
     * // Start the connector ...
     * start(getConnectorClass(), config);
     *
     * // ---------------------------------------------------------------------------------------------------------------
     * // Consume all of the events due to startup and initialization of the database
     * // ---------------------------------------------------------------------------------------------------------------
     * // Testing.Debug.enable();
     * int numCreateDatabase = 1;
     * int numCreateTables = 2;
     * int numDataRecords = databaseDifferences.geometryPointTableRecords() + 2;
     * SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
     * stopConnector();
     * assertThat(records).isNotNull();
     * assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables);
     * assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_222_point")).size()).isEqualTo(databaseDifferences.geometryPointTableRecords());
     * assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_507_geometry")).size()).isEqualTo(2);
     * assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);
     * assertThat(records.databaseNames().size()).isEqualTo(1);
     * assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(
     * numCreateDatabase + numCreateTables);
     * assertThat(records.ddlRecordsForDatabase("regression_test")).isNull();
     * assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
     * assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
     * assertThat(records.ddlRecordsForDatabase("json_test")).isNull();
     * records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);
     *
     * // Check that all records are valid, can be serialized and deserialized ...
     * records.forEach(this::validate);
     * records.forEach(record -> {
     * Struct value = (Struct) record.value();
     * if (record.topic().endsWith("dbz_222_point")) {
     * assertPoint(value);
     * }
     * else if (record.topic().endsWith("dbz_507_geometry")) {
     * assertGeomRecord(value);
     * }
     * });
     * }
     */

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig().build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numTables = 1;
        int numDataRecords = 1;
        int numDdlRecords = numTables * 2 + 3; // for each table (1 drop + 1 create) + for each db (1 create + 1 drop + 1 use)
        int numSetVariables = 1;
        var records = consumeRecordsByTopic(numDdlRecords + numSetVariables + numDataRecords);

        assertThat(records).isNotNull();
        final var dataRecords = records.recordsForTopic(DATABASE.topicForTable("dbz_8157"));
        assertThat(dataRecords).hasSize(1);
        var record = dataRecords.get(0);
        var after = ((Struct) record.value()).getStruct("after");
        assertThat(after.schema().field("f_vector_null").schema().name()).isEqualTo(FloatVector.LOGICAL_NAME);
        assertThat(after.getArray("f_vector_null")).containsExactly(1.1f, 2.2f);
        assertThat(after.getArray("f_vector_default")).containsExactly(11.5f, 22.6f);
        assertThat(after.getArray("f_vector_cons")).containsExactly(31f, 32f);

        stopConnector();
    }
}
