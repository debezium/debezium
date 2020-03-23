/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import io.debezium.connector.base.ChangeEventQueue;

public class SnapshotProcessorTest extends EmbeddedCassandraConnectorTestBase {
    @Test
    public void testSnapshotTable() throws Exception {
        CassandraConnectorContext context = generateTaskContext();
        SnapshotProcessor snapshotProcessor = Mockito.spy(new SnapshotProcessor(context));
        when(snapshotProcessor.isRunning()).thenReturn(true);

        int tableSize = 5;
        context.getCassandraClient().execute("CREATE TABLE IF NOT EXISTS " + keyspaceTable("cdc_table") + " (a int, b text, PRIMARY KEY(a)) WITH cdc = true;");
        context.getCassandraClient().execute("CREATE TABLE IF NOT EXISTS " + keyspaceTable("cdc_table2") + " (a int, b text, PRIMARY KEY(a)) WITH cdc = true;");
        context.getSchemaHolder().refreshSchemas();

        for (int i = 0; i < tableSize; i++) {
            context.getCassandraClient().execute("INSERT INTO " + keyspaceTable("cdc_table") + "(a, b) VALUES (?, ?)", i, String.valueOf(i));
            context.getCassandraClient().execute("INSERT INTO " + keyspaceTable("cdc_table2") + "(a, b) VALUES (?, ?)", i + 10, String.valueOf(i + 10));
        }

        ChangeEventQueue<Event> queue = context.getQueue();
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
        snapshotProcessor.process();
        assertEquals(2 * tableSize, queue.totalCapacity() - queue.remainingCapacity());
        final List<ChangeRecord> table1 = new ArrayList<>();
        final List<ChangeRecord> table2 = new ArrayList<>();
        for (Event event : queue.poll()) {
            ChangeRecord record = (ChangeRecord) event;
            Assert.assertEquals(record.getEventType(), Event.EventType.CHANGE_EVENT);
            Assert.assertEquals(record.getOp(), Record.Operation.INSERT);
            assertEquals(record.getSource().cluster, DatabaseDescriptor.getClusterName());
            assertTrue(record.getSource().snapshot);
            final String tableName = record.getSource().keyspaceTable.name();
            if (tableName.equals(keyspaceTable("cdc_table"))) {
                table1.add(record);
            }
            else {
                table2.add(record);
            }
            Assert.assertEquals(record.getSource().offsetPosition, OffsetPosition.defaultOffsetPosition());
        }
        assertEquals(tableSize, table1.size());
        assertEquals(tableSize, table2.size());
        deleteTestKeyspaceTables();
        deleteTestOffsets(context);
        context.cleanUp();
    }

    @Test
    public void testSnapshotSkipsNonCdcEnabledTable() throws Exception {
        CassandraConnectorContext context = generateTaskContext();
        SnapshotProcessor snapshotProcessor = Mockito.spy(new SnapshotProcessor(context));
        when(snapshotProcessor.isRunning()).thenReturn(true);

        int tableSize = 5;
        context.getCassandraClient().execute("CREATE TABLE IF NOT EXISTS " + keyspaceTable("non_cdc_table") + " (a int, b text, PRIMARY KEY(a)) WITH cdc = false;");
        context.getSchemaHolder().refreshSchemas();
        for (int i = 0; i < tableSize; i++) {
            context.getCassandraClient().execute("INSERT INTO " + keyspaceTable("non_cdc_table") + "(a, b) VALUES (?, ?)", i, String.valueOf(i));
        }

        ChangeEventQueue<Event> queue = context.getQueue();
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
        snapshotProcessor.process();
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());

        deleteTestKeyspaceTables();
        deleteTestOffsets(context);
        context.cleanUp();
    }

    @Test
    public void testSnapshotEmptyTable() throws Exception {
        CassandraConnectorContext context = generateTaskContext();
        AtomicBoolean globalTaskState = new AtomicBoolean(true);
        SnapshotProcessor snapshotProcessor = Mockito.spy(new SnapshotProcessor(context));
        when(snapshotProcessor.isRunning()).thenReturn(true);

        context.getCassandraClient().execute("CREATE TABLE IF NOT EXISTS " + keyspaceTable("cdc_table") + " (a int, b text, PRIMARY KEY(a)) WITH cdc = true;");
        context.getSchemaHolder().refreshSchemas();

        ChangeEventQueue<Event> queue = context.getQueue();
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
        snapshotProcessor.process(); // records empty table to snapshot.offset, so it won't be snapshotted again
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());

        int tableSize = 5;
        for (int i = 0; i < tableSize; i++) {
            context.getCassandraClient().execute("INSERT INTO " + keyspaceTable("cdc_table") + "(a, b) VALUES (?, ?)", i, String.valueOf(i));
        }
        snapshotProcessor.process();
        assertEquals(queue.totalCapacity(), queue.remainingCapacity()); // newly inserted records should be processed by commit log processor instead

        deleteTestKeyspaceTables();
        deleteTestOffsets(context);
        globalTaskState.set(false);
        context.cleanUp();
    }

    @Test
    public void testSnapshotModeAlways() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CassandraConnectorConfig.SNAPSHOT_MODE.name(), "always");
        configs.put(CassandraConnectorConfig.SNAPSHOT_POLL_INTERVAL_MS.name(), "0");
        CassandraConnectorContext context = generateTaskContext(configs);
        SnapshotProcessor snapshotProcessorSpy = Mockito.spy(new SnapshotProcessor(context));
        doNothing().when(snapshotProcessorSpy).snapshot();

        for (int i = 0; i < 5; i++) {
            snapshotProcessorSpy.process();
        }
        verify(snapshotProcessorSpy, times(5)).snapshot();

        context.cleanUp();
    }

    @Test
    public void testSnapshotModeInitial() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CassandraConnectorConfig.SNAPSHOT_MODE.name(), "initial");
        configs.put(CassandraConnectorConfig.SNAPSHOT_POLL_INTERVAL_MS.name(), "0");
        CassandraConnectorContext context = generateTaskContext(configs);
        SnapshotProcessor snapshotProcessorSpy = Mockito.spy(new SnapshotProcessor(context));
        doNothing().when(snapshotProcessorSpy).snapshot();

        for (int i = 0; i < 5; i++) {
            snapshotProcessorSpy.process();
        }
        verify(snapshotProcessorSpy, times(1)).snapshot();

        context.cleanUp();
    }

    @Test
    public void testSnapshotModeNever() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CassandraConnectorConfig.SNAPSHOT_MODE.name(), "never");
        configs.put(CassandraConnectorConfig.SNAPSHOT_POLL_INTERVAL_MS.name(), "0");
        CassandraConnectorContext context = generateTaskContext(configs);
        SnapshotProcessor snapshotProcessorSpy = Mockito.spy(new SnapshotProcessor(context));
        doNothing().when(snapshotProcessorSpy).snapshot();

        for (int i = 0; i < 5; i++) {
            snapshotProcessorSpy.process();
        }
        verify(snapshotProcessorSpy, never()).snapshot();

        context.cleanUp();
    }
}
