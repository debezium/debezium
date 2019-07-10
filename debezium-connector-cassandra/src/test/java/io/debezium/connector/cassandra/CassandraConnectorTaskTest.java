/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CassandraConnectorTaskTest {

    @Test(timeout = 60000)
    public void testProcessorGroup() throws Exception {
        CassandraConnectorTask.ProcessorGroup processorGroup = new CassandraConnectorTask.ProcessorGroup("ProcessorGroup");
        assertEquals("ProcessorGroup", processorGroup.getName());
        AtomicInteger running = new AtomicInteger(0);
        AtomicInteger iteration = new AtomicInteger(0);
        AbstractProcessor processor1 = new AbstractProcessor("processor1", 100) {
            @Override
            public void initialize() {
                running.incrementAndGet();
            }
            @Override
            public void destroy() {
                running.decrementAndGet();
            }

            @Override
            public void process() {
                iteration.incrementAndGet();
            }
        };
        AbstractProcessor processor2 = new AbstractProcessor("processor2", 100) {
            @Override
            public void initialize() {
                running.incrementAndGet();
            }
            @Override
            public void destroy() {
                running.decrementAndGet();
            }

            @Override
            public void process() {
                iteration.incrementAndGet();
            }
        };

        processorGroup.addProcessor(processor1);
        processorGroup.addProcessor(processor2);
        processorGroup.start();
        while (!processor1.isRunning() || !processor2.isRunning()) {
            Thread.sleep(100);
        }
        assertTrue(processorGroup.isRunning());
        assertEquals(2, running.get());
        assertTrue(iteration.get() >= 1);

        processorGroup.terminate();
        assertFalse(processor1.isRunning());
        assertFalse(processor2.isRunning());
        assertEquals(0, running.get());
    }
}
