/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, patch = 5, reason = "MySQL 5.5 does not support CURRENT_TIMESTAMP on DATETIME and only a single column can specify default CURRENT_TIMESTAMP, lifted in MySQL 5.6.5")
public class MySqlTimestampColumnIT extends AbstractConnectorTest {
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-timestamp-column.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("timestampcolumnit", "timestamp_column_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1243")
    public void shouldConvertDateTimeWithZeroPrecision() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("t_user_black_list"))
                .build();

        start(MySqlConnector.class, config);

        // There should be 5 records that imply create database, create table, alter table, insert row, update row.
        // If the ddl parser fails, there will only be 3; the insert/update won't occur.
        SourceRecords records = consumeRecordsByTopic(5);
        assertThat(records.allRecordsInOrder()).hasSize(5);

        // INSERT record
        SourceRecord record = records.allRecordsInOrder().get(3);
        Struct value = ((Struct) record.value());
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        assertThat(value.get("op")).isEqualTo(Envelope.Operation.CREATE.code());
        assertThat(after.get("update_time")).isNotNull();

        // UPDATE record
        record = records.allRecordsInOrder().get(4);
        value = ((Struct) record.value());
        after = value.getStruct(Envelope.FieldName.AFTER);
        assertThat(value.get("op")).isEqualTo(Envelope.Operation.UPDATE.code());
        assertThat(after.get("update_time")).isNotNull();

        stopConnector();
    }
}
