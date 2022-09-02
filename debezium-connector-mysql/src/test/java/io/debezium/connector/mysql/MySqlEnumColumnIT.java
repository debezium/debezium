/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.data.Enum.LOGICAL_NAME;
import static io.debezium.data.Enum.VALUES_FIELD;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope.FieldName;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, patch = 5, reason = "MySQL does not support CURRENT_TIMESTAMP on DATETIME and only a single column can specify default CURRENT_TIMESTAMP, lifted in MySQL 5.6.5")
public class MySqlEnumColumnIT extends AbstractConnectorTest {

    private static final String TYPE_NAME_PARAMETER_KEY = "__debezium.source.column.type";
    private static final String TYPE_LENGTH_PARAMETER_KEY = "__debezium.source.column.length";
    private static final String TYPE_SCALE_PARAMETER_KEY = "__debezium.source.column.scale";

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
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("test_stations_10"))
                .build();

        start(MySqlConnector.class, config);

        // There are 5 records to account for the following operations
        // CREATE DATABASE
        // CREATE TABLE
        // INSERT
        // ALTER TABLE
        // INSERT
        SourceRecords records = consumeRecordsByTopic(5);

        Schema schemaBeforeAlter = records.allRecordsInOrder().get(2).valueSchema().field(FieldName.AFTER).schema();
        Schema schemaAfterAlter = records.allRecordsInOrder().get(4).valueSchema().field(FieldName.AFTER).schema();

        String allowedBeforeAlter = schemaBeforeAlter.field("type").schema().parameters().get(VALUES_FIELD);
        String allowedAfterAlter = schemaAfterAlter.field("type").schema().parameters().get(VALUES_FIELD);

        assertThat(allowedBeforeAlter).isEqualTo("station,post_office");
        assertThat(allowedAfterAlter).isEqualTo("station,post_office,plane,ahihi_dongok,now,test,a\\,b,c\\,'d,g\\,'h");
        stopConnector();
    }

    @Test
    @FixFor("DBZ-1636")
    public void shouldPropagateColumnSourceType() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("test_stations_10"))
                .with("column.propagate.source.type", DATABASE.qualifiedTableName("test_stations_10") + ".type")
                .build();

        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(5);

        SourceRecord recordBefore = records.allRecordsInOrder().get(2);
        Schema schemaBeforeAlter = recordBefore.valueSchema().field(FieldName.AFTER).schema();

        Schema typeBeforeSchema = schemaBeforeAlter.field("type").schema();
        assertThat(typeBeforeSchema.name()).isEqualTo(LOGICAL_NAME);

        Map<String, String> beforeParameters = typeBeforeSchema.parameters();
        assertThat(beforeParameters.get(TYPE_NAME_PARAMETER_KEY)).isEqualTo("ENUM");
        assertThat(beforeParameters.get(TYPE_LENGTH_PARAMETER_KEY)).isEqualTo("1");
        assertThat(beforeParameters.get(TYPE_SCALE_PARAMETER_KEY)).isNull();

        SourceRecord recordAfter = records.allRecordsInOrder().get(4);
        Schema schemaAfterAlter = recordAfter.valueSchema().field(FieldName.AFTER).schema();

        Schema typeAfterSchema = schemaAfterAlter.field("type").schema();
        assertThat(typeAfterSchema.name()).isEqualTo(LOGICAL_NAME);

        Map<String, String> afterParameters = schemaAfterAlter.field("type").schema().parameters();
        assertThat(afterParameters.get(TYPE_NAME_PARAMETER_KEY)).isEqualTo("ENUM");
        assertThat(afterParameters.get(TYPE_LENGTH_PARAMETER_KEY)).isEqualTo("1");
        assertThat(afterParameters.get(TYPE_SCALE_PARAMETER_KEY)).isNull();

        stopConnector();
    }
}
