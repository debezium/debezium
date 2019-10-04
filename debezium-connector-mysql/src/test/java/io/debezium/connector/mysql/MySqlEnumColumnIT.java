/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;

import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
public class MySqlEnumColumnIT extends AbstractConnectorTest {
    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-enum-column.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("enumcolumnit", "enum_column_test").withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1203")
    public void shouldAlterEnumColumnCharacterSet() throws Exception {

        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("test_stations_10"))
                .build();

        start(MySqlConnector.class, config);

        // There are 5 records to account for the following operations
        // CREATE DATABASE
        // CREATE TABLE
        // INSERT
        // ALTER TABLE
        // INSERT
        SourceRecords records = consumeRecordsByTopic( 5 );

        Schema schemaBeforeAlter = records.allRecordsInOrder().get(2).valueSchema().field("after").schema();
        Schema schemaAfterAlter = records.allRecordsInOrder().get(4).valueSchema().field("after").schema();

        String allowedBeforeAlter = schemaBeforeAlter.field("type").schema().parameters().get("allowed");
        String allowedAfterAlter = schemaAfterAlter.field("type").schema().parameters().get("allowed");

        assertThat(allowedBeforeAlter).isEqualTo("station,post_office");
        assertThat(allowedAfterAlter).isEqualTo("station,post_office,plane,ahihi_dongok,now,test,a\\,b,c\\,'d,g\\,'h");
        stopConnector();
    }
}
