/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.time.Conversions;

public class FileOffsetWriterTest {

    private Path offsetDir;
    private OffsetWriter offsetWriter;
    private Properties snapshotProps;
    private Properties commitLogProps;

    @Before
    public void setUp() throws IOException {
        offsetDir = Files.createTempDirectory("offset");
        offsetWriter = new FileOffsetWriter(offsetDir.toAbsolutePath().toString());
        snapshotProps = new Properties();
        commitLogProps = new Properties();
    }

    @Test
    public void testMarkOffset() {
        ChangeRecord snapshotRecord = generateRecord(true, true,
                new OffsetPosition("", -1),
                new KeyspaceTable("test_keyspace", "test_table"));
        ChangeRecord commitLogRecord = generateRecord(true, false,
                new OffsetPosition("CommitLog-6-12345.log", 100),
                new KeyspaceTable("test_keyspace", "test_table"));
        ChangeRecord commitLogRecordDupe = generateRecord(true, false,
                new OffsetPosition("CommitLog-6-12345.log", 100),
                new KeyspaceTable("test_keyspace", "test_table"));
        ChangeRecord commitLogRecordOlderLog = generateRecord(true, false,
                new OffsetPosition("CommitLog-6-12344.log", 101),
                new KeyspaceTable("test_keyspace", "test_table"));
        ChangeRecord commitLogRecordDiffTable = generateRecord(true, false,
                new OffsetPosition("CommitLog-6-12345.log", 100),
                new KeyspaceTable("test_keyspace", "test_another_table"));

        assertFalse(isProcessed(snapshotRecord));
        process(snapshotRecord);
        assertTrue(isProcessed(snapshotRecord));

        assertFalse(isProcessed(commitLogRecord));
        process(commitLogRecord);
        assertTrue(isProcessed(commitLogRecord));

        assertTrue(isProcessed(commitLogRecordDupe));
        process(commitLogRecordDupe);
        assertTrue(isProcessed(commitLogRecordDupe));

        assertTrue(isProcessed(commitLogRecordOlderLog));
        process(commitLogRecordOlderLog);
        assertTrue(isProcessed(commitLogRecordOlderLog));
        // make sure the later record one is still processed
        assertTrue(isProcessed(commitLogRecord));

        assertFalse(isProcessed(commitLogRecordDiffTable));
        process(commitLogRecordDiffTable);
        assertTrue(isProcessed(commitLogRecordDiffTable));
    }

    @Test
    public void testFlush() throws IOException {
        offsetWriter.flush();
        try (FileInputStream fis = new FileInputStream(offsetDir.toString() + "/" + FileOffsetWriter.SNAPSHOT_OFFSET_FILE)) {
            snapshotProps.load(fis);
        }
        try (FileInputStream fis = new FileInputStream(offsetDir.toString() + "/" + FileOffsetWriter.COMMITLOG_OFFSET_FILE)) {
            commitLogProps.load(fis);
        }
        assertEquals(0, snapshotProps.size());
        assertEquals(0, commitLogProps.size());

        ChangeRecord snapshotRecord = generateRecord(true, true,
                new OffsetPosition("", -1),
                new KeyspaceTable("test_keyspace", "test_table"));
        ChangeRecord commitLogRecord = generateRecord(true, false,
                new OffsetPosition("CommitLog-6-12345.log", 100),
                new KeyspaceTable("test_keyspace", "test_table"));
        ChangeRecord commitLogRecordDiffTable = generateRecord(true, false,
                new OffsetPosition("CommitLog-6-12345.log", 100),
                new KeyspaceTable("test_keyspace", "test_another_table"));

        process(snapshotRecord);
        process(commitLogRecord);
        process(commitLogRecordDiffTable);

        offsetWriter.flush();
        try (FileInputStream fis = new FileInputStream(offsetDir.toString() + "/" + FileOffsetWriter.SNAPSHOT_OFFSET_FILE)) {
            snapshotProps.load(fis);
        }
        try (FileInputStream fis = new FileInputStream(offsetDir.toString() + "/" + FileOffsetWriter.COMMITLOG_OFFSET_FILE)) {
            commitLogProps.load(fis);
        }
        assertEquals(1, snapshotProps.size());
        assertEquals(2, commitLogProps.size());
        assertEquals(OffsetPosition.defaultOffsetPosition().serialize(),
                snapshotProps.getProperty(new KeyspaceTable("test_keyspace", "test_table").name()));
        assertEquals(new OffsetPosition("CommitLog-6-12345.log", 100).serialize(),
                commitLogProps.getProperty(new KeyspaceTable("test_keyspace", "test_table").name()));
        assertEquals(new OffsetPosition("CommitLog-6-12345.log", 100).serialize(),
                commitLogProps.getProperty(new KeyspaceTable("test_keyspace", "test_another_table").name()));
    }

    @Test(expected = CassandraConnectorTaskException.class)
    public void testTwoFileWriterCannotCoexist() throws IOException {
        new FileOffsetWriter(offsetDir.toAbsolutePath().toString());
    }

    private ChangeRecord generateRecord(boolean markOffset, boolean isSnapshot, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable) {
        CassandraConnectorConfig config = new CassandraConnectorConfig(Configuration.from(new Properties()));
        SourceInfo sourceInfo = new SourceInfo(config, "test-cluster", offsetPosition, keyspaceTable,
                isSnapshot, Conversions.toInstantFromMicros(System.currentTimeMillis() * 1000));
        return new ChangeRecord(sourceInfo, new RowData(), Schema.INT32_SCHEMA, Schema.INT32_SCHEMA, Record.Operation.INSERT, markOffset);
    }

    private boolean isProcessed(ChangeRecord record) {
        return offsetWriter.isOffsetProcessed(
                record.getSource().keyspaceTable.name(),
                record.getSource().offsetPosition.serialize(),
                record.getSource().snapshot);
    }

    private void process(ChangeRecord record) {
        offsetWriter.markOffset(
                record.getSource().keyspaceTable.name(),
                record.getSource().offsetPosition.serialize(),
                record.getSource().snapshot);
    }
}
