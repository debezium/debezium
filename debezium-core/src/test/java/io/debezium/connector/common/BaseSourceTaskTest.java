/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

public class BaseSourceTaskTest {

    private final MyBaseSourceTask baseSourceTask = new MyBaseSourceTask();

    @Before
    public void setup() {
        baseSourceTask.initialize(mock(SourceTaskContext.class));
    }

    @Test
    public void verifyTaskStartsAndStops() throws InterruptedException {

        baseSourceTask.start(new HashMap<>());
        assertEquals(BaseSourceTask.State.INITIAL, baseSourceTask.getTaskState());
        baseSourceTask.poll();
        assertEquals(BaseSourceTask.State.RUNNING, baseSourceTask.getTaskState());
        baseSourceTask.stop();
        assertEquals(BaseSourceTask.State.STOPPED, baseSourceTask.getTaskState());

        assertEquals(1, baseSourceTask.startCount.get());
        assertEquals(1, baseSourceTask.stopCount.get());
        verify(baseSourceTask.coordinator).stop();
    }

    @Test
    public void verifyStartAndStopWithoutPolling() {
        baseSourceTask.initialize(mock(SourceTaskContext.class));
        baseSourceTask.start(new HashMap<>());
        assertEquals(BaseSourceTask.State.INITIAL, baseSourceTask.getTaskState());
        baseSourceTask.stop();
        assertEquals(BaseSourceTask.State.STOPPED, baseSourceTask.getTaskState());

        assertEquals(0, baseSourceTask.startCount.get());
        assertEquals(1, baseSourceTask.stopCount.get());
    }

    @Test
    public void verifyTaskCanBeStartedAfterStopped() throws InterruptedException {

        baseSourceTask.start(new HashMap<>());
        assertEquals(BaseSourceTask.State.INITIAL, baseSourceTask.getTaskState());
        baseSourceTask.poll();
        assertEquals(BaseSourceTask.State.RUNNING, baseSourceTask.getTaskState());
        baseSourceTask.stop();
        assertEquals(BaseSourceTask.State.STOPPED, baseSourceTask.getTaskState());
        baseSourceTask.start(new HashMap<>());
        assertEquals(BaseSourceTask.State.INITIAL, baseSourceTask.getTaskState());
        baseSourceTask.poll();
        assertEquals(BaseSourceTask.State.RUNNING, baseSourceTask.getTaskState());
        baseSourceTask.stop();
        assertEquals(BaseSourceTask.State.STOPPED, baseSourceTask.getTaskState());

        assertEquals(2, baseSourceTask.startCount.get());
        assertEquals(2, baseSourceTask.stopCount.get());
        verify(baseSourceTask.coordinator, times(2)).stop();
    }

    @Test
    public void verifyTaskRestartsSuccessfully() throws InterruptedException {
        MyBaseSourceTask baseSourceTask = new MyBaseSourceTask() {
            @Override
            protected ChangeEventSourceCoordinator<Partition, OffsetContext> start(Configuration config) {
                ChangeEventSourceCoordinator<Partition, OffsetContext> result = super.start(config);
                if (startCount.get() < 3) {
                    throw new RetriableException("Retry " + startCount.get());
                }

                return result;
            }
        };

        baseSourceTask.initialize(mock(SourceTaskContext.class));
        Map<String, String> config = Map.of(
                CommonConnectorConfig.RETRIABLE_RESTART_WAIT.name(), "1" // wait 1ms between restarts
        );
        baseSourceTask.start(config);
        assertEquals(BaseSourceTask.State.INITIAL, baseSourceTask.getTaskState());
        pollAndIgnoreRetryException(baseSourceTask);
        assertEquals(BaseSourceTask.State.RESTARTING, baseSourceTask.getTaskState());
        sleep(100); // wait 10ms in order to satisfy retriable wait
        pollAndIgnoreRetryException(baseSourceTask);
        assertEquals(BaseSourceTask.State.RESTARTING, baseSourceTask.getTaskState());
        sleep(100); // wait 10ms in order to satisfy retriable wait
        baseSourceTask.poll();
        assertEquals(BaseSourceTask.State.RUNNING, baseSourceTask.getTaskState());
        baseSourceTask.stop();
        assertEquals(BaseSourceTask.State.STOPPED, baseSourceTask.getTaskState());

        assertEquals(3, baseSourceTask.startCount.get());
        assertEquals(3, baseSourceTask.stopCount.get());
        verify(baseSourceTask.coordinator, times(1)).stop();
    }

    @Test
    public void verifyOutOfOrderPollDoesNotStartTask() throws InterruptedException {
        baseSourceTask.start(new HashMap<>());
        assertEquals(BaseSourceTask.State.INITIAL, baseSourceTask.getTaskState());
        baseSourceTask.stop();
        assertEquals(BaseSourceTask.State.STOPPED, baseSourceTask.getTaskState());
        baseSourceTask.poll();
        assertEquals(BaseSourceTask.State.STOPPED, baseSourceTask.getTaskState());

        assertEquals(0, baseSourceTask.startCount.get());
        assertEquals(1, baseSourceTask.stopCount.get());
    }

    private static void pollAndIgnoreRetryException(BaseSourceTask<Partition, OffsetContext> baseSourceTask) throws InterruptedException {
        try {
            baseSourceTask.poll();
        }
        catch (RetriableException e) {
            // nothing to do
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException();
        }
    }

    public static class MyBaseSourceTask extends BaseSourceTask<Partition, OffsetContext> {
        final List<SourceRecord> records = new ArrayList<>();
        final AtomicInteger startCount = new AtomicInteger();
        final AtomicInteger stopCount = new AtomicInteger();

        @SuppressWarnings("unchecked")
        final ChangeEventSourceCoordinator<Partition, OffsetContext> coordinator = mock(ChangeEventSourceCoordinator.class);

        @Override
        protected ChangeEventSourceCoordinator<Partition, OffsetContext> start(Configuration config) {
            startCount.incrementAndGet();
            return coordinator;
        }

        @Override
        protected List<SourceRecord> doPoll() {
            return records;
        }

        @Override
        protected void doStop() {
            stopCount.incrementAndGet();
        }

        @Override
        protected Iterable<Field> getAllConfigurationFields() {
            return List.of(Field.create("f1"));
        }

        @Override
        public String version() {
            return "1.0";
        }
    }
}
