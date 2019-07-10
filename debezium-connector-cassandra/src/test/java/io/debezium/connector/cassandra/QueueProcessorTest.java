/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.connector.cassandra.transforms.CassandraTypeToAvroSchemaMapper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class QueueProcessorTest extends EmbeddedCassandraConnectorTestBase {
    private CassandraConnectorContext context;
    private QueueProcessor queueProcessor;
    private KafkaRecordEmitter emitter;

    @Before
    public void setUp() throws Exception {
        context = generateTaskContext();
        emitter = mock(KafkaRecordEmitter.class);
        queueProcessor = new QueueProcessor(context, emitter);
    }

    @After
    public void tearDown() {
        context.cleanUp();
    }

    @Test
    public void testProcessChangeRecords() throws Exception {
        doNothing().when(emitter).emit(any());

        int recordSize = 5;
        BlockingEventQueue<Event> queue = context.getQueue();
        for (int i = 0; i < recordSize; i++) {
            SourceInfo sourceInfo = new SourceInfo(DatabaseDescriptor.getClusterName(), new OffsetPosition("CommitLog-6-123.log", i), new KeyspaceTable(TEST_KEYSPACE, "cdc_table"), false, System.currentTimeMillis() * 1000);
            Record record = new ChangeRecord(sourceInfo, new RowData(), CassandraTypeToAvroSchemaMapper.INT_TYPE,  CassandraTypeToAvroSchemaMapper.INT_TYPE, Record.Operation.INSERT, false);
            queue.enqueue(record);
        }

        assertEquals(recordSize, queue.size());
        queueProcessor.process();
        verify(emitter, times(recordSize)).emit(any());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testProcessTombstoneRecords() throws Exception {
        doNothing().when(emitter).emit(any());

        int recordSize = 5;
        BlockingEventQueue<Event> queue = context.getQueue();
        for (int i = 0; i < recordSize; i++) {
            SourceInfo sourceInfo = new SourceInfo(DatabaseDescriptor.getClusterName(), new OffsetPosition("CommitLog-6-123.log", i), new KeyspaceTable(TEST_KEYSPACE, "cdc_table"), false, System.currentTimeMillis() * 1000);
            Record record = new TombstoneRecord(sourceInfo, new RowData(), CassandraTypeToAvroSchemaMapper.INT_TYPE);
            queue.enqueue(record);
        }

        assertEquals(recordSize, queue.size());
        queueProcessor.process();
        verify(emitter, times(recordSize)).emit(any());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testProcessEofEvent() throws Exception {
        doNothing().when(emitter).emit(any());

        BlockingEventQueue<Event> queue = context.getQueue();
        File commitLogFile = generateCommitLogFile();
        queue.enqueue(new EOFEvent(commitLogFile, true));

        assertEquals(1, queue.size());
        queueProcessor.process();
        verify(emitter, times(0)).emit(any());
        assertTrue(queue.isEmpty());
    }
}
