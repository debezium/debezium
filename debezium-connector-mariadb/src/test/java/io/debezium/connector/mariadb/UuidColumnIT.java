/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Uuid;
import io.debezium.doc.FixFor;
import io.debezium.schema.SchemaFactory;

/**
 * @author Wouter Coekaerts
 */
public class UuidColumnIT extends AbstractBinlogConnectorIT<MariaDbConnector> implements MariaDbCommon {
    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-uuid-column.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("uuidcolumnit", "uuid_column_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

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
    }

    @Test
    @FixFor("DBZ-9027")
    public void shouldHandleUuidStreaming() throws Exception {
        shouldHandleUuid(SnapshotMode.NEVER);
    }

    @Test
    @FixFor("DBZ-9027")
    public void shouldHandleUuidSnapshot() throws Exception {
        shouldHandleUuid(SnapshotMode.INITIAL);
    }

    private void shouldHandleUuid(SnapshotMode snapshotMode) throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("test_uuid"))
                .build();

        start(getConnectorClass(), config);

        int insertRecordCount = 4; // number of insert statements in uuid_column_test.sql
        List<SourceRecord> records = consumeSkippingSchemaChanges(insertRecordCount);

        // verify the schema
        Schema schema = records.get(0).valueSchema().field(FieldName.AFTER).schema();
        assertThat(schema.field("id").schema()).isEqualTo(Uuid.schema());

        // verify all the values: the UUID value should equal the value put in the "expected" char column
        for (SourceRecord insertRecord : records) {
            Struct after = ((Struct) insertRecord.value()).getStruct("after");
            String expected = after.getString("expected");
            assertThat(after.getString("id")).isEqualTo(expected);
        }

        stopConnector();
    }

    /** Consume numberOfRecords, but skipping SchemaChange events */
    private List<SourceRecord> consumeSkippingSchemaChanges(int numberOfRecords) throws InterruptedException {
        List<SourceRecord> recordList = new ArrayList<>();
        consumeRecordsUntil((i, r) -> recordList.size() == numberOfRecords, (i, r) -> "", 3,
                r -> {
                    if (!((Struct) r.value()).schema().name().endsWith(SchemaFactory.SCHEMA_CHANGE_VALUE)) {
                        recordList.add(r);
                    }
                }, true);
        assertThat(recordList).hasSize(numberOfRecords);
        return recordList;
    }
}
