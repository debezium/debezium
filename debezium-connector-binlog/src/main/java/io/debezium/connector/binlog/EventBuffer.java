/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;

import io.debezium.connector.binlog.BinlogStreamingChangeEventSource.BinlogPosition;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;

/**
 * This class represents a look-ahead buffer that allows Debezium to accumulate binlog events and decide
 * if the last event in transaction is either {@code ROLLBACK} or {@code COMMIT}. The incoming events are either
 * supposed to be in transaction or out-of-transaction. When out-of-transaction they are sent directly into
 * the destination handler. When in transaction the event goes through the buffering.
 * <p>
 * The reason for the buffering is that the binlog contains rolled back transactions in some cases. E.g. that's
 * the case when a temporary table is dropped (see DBZ-390). For rolled back transactions we may not propagate
 * any of the contained events, hence the buffering is applied.
 * <p>
 * The transaction start is identified by a {@code BEGIN} event. Transaction is ended either by {@code COMMIT}
 * event or by {@code XID} an event.
 * <p>
 * If there are more events that can fit to the buffer then:
 * <ul>
 *     <li>Binlog position is recorded for the first event not fitting into the buffer</li>
 *     <li>Binlog position is recorded for the commit event</li>
 *     <li>Buffer content is sent to the final handler</li>
 *     <li>Binlog position is rewound and all events between the above recorded positions are sent to the final handler</li>
 * </ul>
 *
 * @author Jiri Pechanec
 * @author Chris Cranford
 */
public class EventBuffer<T extends BinlogStreamingChangeEventSource<P, O>, P extends BinlogPartition, O extends BinlogOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventBuffer.class);

    private final int capacity;
    private final Queue<Event> buffer;
    private final T streamingChangeEventSource;
    private final ChangeEventSourceContext changeEventSourceContext;
    private boolean txStarted = false;

    /**
     * Contains the position of the first event that has not fit into the buffer.
     */
    private BinlogPosition largeTxNotBufferedPosition;

    /**
     * Contains the position of the last event belonging to the transaction that has not fit into the buffer.
     */
    private BinlogPosition forwardTillPosition;

    public EventBuffer(int capacity, T streamingChangeEventSource, ChangeEventSourceContext changeEventSourceContext) {
        this.capacity = capacity;
        this.buffer = new ArrayBlockingQueue<>(capacity);
        this.streamingChangeEventSource = streamingChangeEventSource;
        this.changeEventSourceContext = changeEventSourceContext;
    }

    /**
     * An entry point to the buffer that should be used by the streaming change event source to push events.
     *
     * @param partition the partition to which the events belong
     * @param offsetContext the offset context
     * @param event event to be stored in the buffer
     */
    public void add(P partition, O offsetContext, Event event) {
        if (event == null) {
            return;
        }

        // we're reprocessing events of the current TX between the position where the
        // buffer was full and the end of the TX; in this case there's nothing to do
        // besides directly emitting the events
        if (isReplayingEventsBeyondBufferCapacity()) {
            streamingChangeEventSource.handleEvent(partition, offsetContext, changeEventSourceContext, event);
            return;
        }

        if (event.getHeader().getEventType() == EventType.QUERY) {
            QueryEventData command = streamingChangeEventSource.unwrapData(event);
            LOGGER.debug("Received query command: {}", event);
            String sql = command.getSql().trim();
            if (sql.equalsIgnoreCase("BEGIN")) {
                beginTransaction(partition, offsetContext, event);
            }
            else if (sql.equalsIgnoreCase("COMMIT")) {
                completeTransaction(partition, offsetContext, true, event);
            }
            else if (sql.equalsIgnoreCase("ROLLBACK")) {
                rollbackTransaction();
            }
            else {
                consumeEvent(partition, offsetContext, event);
            }
        }
        else if (event.getHeader().getEventType() == EventType.MARIADB_GTID) {
            // When the GTID_EVENT has flag 1 set (meaning there is no following commit),
            // then we don't create a new transaction for this. This typically happens
            // for DDL operations which are always transaction scoped.
            MariadbGtidEventData gtidEventData = (MariadbGtidEventData) event.getData();
            if ((gtidEventData.getFlags() & 0x01) != 0x01) {
                // signals a new transaction for MariaDB, treat like QUERY events with BEGIN
                beginTransaction(partition, offsetContext, event);
            }
        }
        else if (event.getHeader().getEventType() == EventType.XID) {
            completeTransaction(partition, offsetContext, true, event);
        }
        else {
            consumeEvent(partition, offsetContext, event);
        }
    }

    /**
     * Whether we are replaying TX events from binlog that have not fit into the buffer before
     */
    private boolean isReplayingEventsBeyondBufferCapacity() {
        if (forwardTillPosition != null) {
            if (forwardTillPosition.equals(streamingChangeEventSource.getCurrentBinlogPosition())) {
                forwardTillPosition = null;
            }
            return true;
        }
        return false;
    }

    /**
     * Adds an event to the buffer if there is a space available. Records binlog position for the first
     * event that does not fit for later replay.
     *
     * @param event the event
     */
    private void addToBuffer(Event event) {
        if (isInBufferFullMode()) {
            return;
        }
        if (buffer.size() == capacity) {
            switchToBufferFullMode();
        }
        else {
            buffer.add(event);
        }
    }

    /**
     * Switches the buffer to operating in full-mode.
     */
    private void switchToBufferFullMode() {
        largeTxNotBufferedPosition = streamingChangeEventSource.getCurrentBinlogPosition();
        LOGGER.info("Buffer full, will need to re-read part of the transaction from binlog from {}", largeTxNotBufferedPosition);
        streamingChangeEventSource.getMetrics().onLargeTransaction();
        // Position for TABLE_MAP is not stored by com.github.shyiko.mysql.binlog.BinaryLogClient.updateClientBinlogFilenameAndPosition(Event)
        if (buffer.peek().getHeader().getEventType() == EventType.TABLE_MAP) {
            buffer.remove();
        }
    }

    /**
     * @return true if the buffer is operating in full mode; false otherwise
     */
    private boolean isInBufferFullMode() {
        return largeTxNotBufferedPosition != null;
    }

    /**
     * Consumes the specific event.
     *
     * @param partition the partition where the event was committed
     * @param offsetContext the offset context
     * @param event the event
     */
    private void consumeEvent(P partition, O offsetContext, Event event) {
        if (txStarted) {
            addToBuffer(event);
        }
        else {
            streamingChangeEventSource.handleEvent(partition, offsetContext, changeEventSourceContext, event);
        }
    }

    /**
     * Starts the transaction
     *
     * @param partition the partition where the transaction was committed
     * @param offsetContext the offset context
     * @param event the event
     */
    private void beginTransaction(P partition, O offsetContext, Event event) {
        if (txStarted) {
            LOGGER.warn("New transaction started but the previous was not completed, processing the buffer");
            completeTransaction(partition, offsetContext, false, null);
        }
        else {
            txStarted = true;
        }
        addToBuffer(event);
    }

    /**
     * Sends all events from the buffer int a final handler. For large transactions it executes rewind
     * of binlog reader back to the first event that was not stored in the buffer.
     *
     * @param partition the partition where the transaction was committed
     * @param offsetContext the offset context
     * @param wellFormed true if the transaction is well-formed, false otherwise
     * @param event the event
     */
    private void completeTransaction(P partition, O offsetContext, boolean wellFormed, Event event) {
        LOGGER.debug("Committing transaction");
        if (event != null) {
            addToBuffer(event);
        }
        if (!txStarted) {
            LOGGER.warn("Commit requested but TX was not started before");
            wellFormed = false;
        }
        LOGGER.debug("Executing events from buffer");
        for (Event e : buffer) {
            streamingChangeEventSource.handleEvent(partition, offsetContext, changeEventSourceContext, e);
        }
        LOGGER.debug("Executing events from binlog that have not fit into buffer");
        if (isInBufferFullMode()) {
            forwardTillPosition = streamingChangeEventSource.getCurrentBinlogPosition();
            streamingChangeEventSource.rewindBinaryLogClient(changeEventSourceContext, largeTxNotBufferedPosition);
        }
        streamingChangeEventSource.getMetrics().onCommittedTransaction();
        if (!wellFormed) {
            streamingChangeEventSource.getMetrics().onNotWellFormedTransaction();
        }
        clear();
    }

    /**
     * Rollback the transaction
     */
    private void rollbackTransaction() {
        LOGGER.debug("Rolling back transaction");
        boolean wellFormed = true;
        if (!txStarted) {
            LOGGER.warn("Rollback requested but TX was not started before");
            wellFormed = false;
        }
        streamingChangeEventSource.getMetrics().onRolledBackTransaction();
        if (!wellFormed) {
            streamingChangeEventSource.getMetrics().onNotWellFormedTransaction();
        }
        clear();
    }

    /**
     * Cleans-up the buffer after the transaction is either thrown away or streamed into a Kafka topic
     */
    private void clear() {
        buffer.clear();
        largeTxNotBufferedPosition = null;
        txStarted = false;
    }
}
