/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CassandraConnectorTaskTest {

    @Test(timeout = 60000)
    public void testProcessorGroup() throws Exception {
        CassandraConnectorTask.ProcessorGroup processorGroup = new CassandraConnectorTask.ProcessorGroup("ProcessorGroup");
        assertEquals("ProcessorGroup", processorGroup.getName());
        AtomicBoolean taskState = processorGroup.getTaskState();
        AtomicInteger running = new AtomicInteger(0);
        AbstractProcessor processor1 = new AbstractProcessor("processor1", taskState) {
            @Override
            public void doStart() throws InterruptedException {
                running.incrementAndGet();
                while (isTaskRunning()) {
                    Thread.sleep(100);
                }
            }
            @Override
            public void doStop() {
                running.decrementAndGet();
            }
        };
        AbstractProcessor processor2 = new AbstractProcessor("processor2", taskState) {
            @Override
            public void doStart() throws InterruptedException {
                running.incrementAndGet();
                while (isTaskRunning()) {
                    Thread.sleep(100);
                }
            }
            @Override
            public void doStop() {
                running.decrementAndGet();
            }
        };

        processorGroup.addProcessor(processor1);
        processorGroup.addProcessor(processor2);
        processorGroup.start();
        while (!processor1.isProcessorRunning() || !processor2.isProcessorRunning()) {
            Thread.sleep(100);
        }
        assertTrue(processorGroup.isRunning());
        assertEquals(2, running.get());
        processor1.stop();
        assertFalse(processor1.isProcessorRunning());
        // stopping processor1 should terminate processor2 eventually
        while (processor2.isProcessorRunning()) {
            Thread.sleep(100);
        }
        assertEquals(0, running.get());
        processorGroup.terminate();
    }
}
