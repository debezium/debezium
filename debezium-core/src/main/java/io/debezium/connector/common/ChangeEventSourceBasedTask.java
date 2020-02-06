/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ChangeEventSourceFacade;
import io.debezium.pipeline.DataChangeEvent;

public abstract class ChangeEventSourceBasedTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventSourceBasedTask.class);

    private enum State {
        RUNNING,
        STOPPED;
    }

    private final AtomicReference<State> state = new AtomicReference<State>(State.STOPPED);
    private ChangeEventSourceFacade eventSourceFacade;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile Map<String, ?> lastOffset;

    @Override
    protected final ChangeEventSourceCoordinator start(Configuration config) {
        if (!state.compareAndSet(State.STOPPED, State.RUNNING)) {
            LOGGER.info("Connector has already been started");
            return null;
        }

        eventSourceFacade = createEventSourceFacade(config);
        eventSourceFacade.start();
        queue = eventSourceFacade.getQueue();
        return eventSourceFacade.getCoordinator();
    }

    protected abstract ChangeEventSourceFacade createEventSourceFacade(Configuration config);

    @Override
    public void commit() {
        if (eventSourceFacade != null) {
            eventSourceFacade.commitOffset(lastOffset);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        if (!sourceRecords.isEmpty()) {
            this.lastOffset = sourceRecords.get(sourceRecords.size() - 1).sourceOffset();
        }

        return sourceRecords;
    }

    @Override
    public final void stop() {
        if (!state.compareAndSet(State.RUNNING, State.STOPPED)) {
            LOGGER.info("Connector has already been stopped");
            return;
        }

        if (eventSourceFacade != null) {
            eventSourceFacade.stop();
        }
    }
}
