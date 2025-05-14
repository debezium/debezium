/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;

/**
 * @author Gunnar Morling
 */
public abstract class BinlogTableMaintenanceStatementsIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-table-maintenance.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("tablemaintenanceit", "table_maintenance_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
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

        dropAllDatabases();
    }

    @Test
    @FixFor("DBZ-253")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 1;
        int numTableMaintenanceStatements = 3;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numTableMaintenanceStatements);
        System.out.println(records.allRecordsInOrder());
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables + numTableMaintenanceStatements);
        assertThat(records.databaseNames()).containsOnly(DATABASE.getDatabaseName());
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(
                numCreateDatabase + numCreateTables + numTableMaintenanceStatements);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
    }
}
