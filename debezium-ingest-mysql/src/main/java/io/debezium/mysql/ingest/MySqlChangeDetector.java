/*
 * Copyright 2016 Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;

import io.debezium.config.Configuration;
import io.debezium.mysql.MySqlConfiguration;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 * 
 * @see MySqlConnector
 * @author Randall Hauch
 */
public class MySqlChangeDetector extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final EnumMap<EventType, EventHandler> eventHandlers = new EnumMap<>(EventType.class);
    private final Tables tables;
    private final TableConverters tableConverters;

    // These are all effectively constants between start(...) and stop(...)
    private BinaryLogClient client;
    private BlockingQueue<Event> events;
    private List<Event> batchEvents;
    private int maxBatchSize;
    private long pollIntervalMs;

    // Used in the methods that process events ...
    private final SourceInfo sourceInfo = new SourceInfo();

    public MySqlChangeDetector() {
        this(null);
    }

    public MySqlChangeDetector( TopicSelector topicSelector ) {
        topicSelector = topicSelector != null ? topicSelector : TopicSelector.defaultSelector();
        tables = new Tables();
        tableConverters = new TableConverters(topicSelector, tables, this::signalTablesChanged);
        eventHandlers.put(EventType.TABLE_MAP, tableConverters::updateTableMetadata);
        eventHandlers.put(EventType.QUERY, tableConverters::updateTableCommand);
        eventHandlers.put(EventType.EXT_WRITE_ROWS, tableConverters::handleInsert);
        eventHandlers.put(EventType.EXT_UPDATE_ROWS, tableConverters::handleUpdate);
        eventHandlers.put(EventType.EXT_DELETE_ROWS, tableConverters::handleDelete);
    }

    @Override
    public String version() {
        return Module.version();
    }
    
    protected void signalTablesChanged( Set<TableId> changedTables ) {
        // TODO: do something
    }

    @Override
    public void start(Map<String, String> props) {
        // Read and verify the configuration ...
        final Configuration config = Configuration.from(props);
        final String user = config.getString(MySqlConfiguration.USER);
        final String password = config.getString(MySqlConfiguration.PASSWORD);
        final String host = config.getString(MySqlConfiguration.HOSTNAME);
        final int port = config.getInteger(MySqlConfiguration.PORT);
        final Long serverId = config.getLong(MySqlConfiguration.SERVER_ID);
        final String logicalId = config.getString(MySqlConfiguration.LOGICAL_ID.name(), "" + host + ":" + port);
        final boolean keepAlive = config.getBoolean(MySqlConfiguration.KEEP_ALIVE);
        final int maxQueueSize = config.getInteger(MySqlConfiguration.MAX_QUEUE_SIZE);
        final long timeoutInMilliseconds = config.getLong(MySqlConfiguration.CONNECTION_TIMEOUT_MS);
        maxBatchSize = config.getInteger(MySqlConfiguration.MAX_BATCH_SIZE);
        pollIntervalMs = config.getLong(MySqlConfiguration.POLL_INTERVAL_MS);
        
        // Create the queue ...
        events = new LinkedBlockingDeque<>(maxQueueSize);
        batchEvents = new ArrayList<>(maxBatchSize);

        // Set up the log reader ...
        client = new BinaryLogClient(host, port, user, password);
        client.setServerId(serverId);
        client.setKeepAlive(keepAlive);
        if (logger.isDebugEnabled()) client.registerEventListener(this::logEvent);
        client.registerEventListener(this::enqueue);
        client.registerLifecycleListener(traceLifecycleListener());

        // Check if we've already processed some of the log for this database ...
        sourceInfo.setDatabase(logicalId);
        if (context != null) {
            // TODO: Figure out how to load the table definitions from previous runs. Can it be read from each of the output
            // topics? Does it need to be serialized locally?
            
            // Get the offsets for our partition ...
            sourceInfo.setOffset(context.offsetStorageReader().offset(sourceInfo.partition()));
            // And set the client to start from that point ...
            client.setBinlogFilename(sourceInfo.binlogFilename());
            client.setBinlogPosition(sourceInfo.binlogPosition());
            // The event row number will be used when processing the first event ...
        } else {
            // initializes this position, though it will be reset when we see the first event (should be a rotate event) ...
            sourceInfo.setBinlogPosition(client.getBinlogPosition());
        }

        // Start the log reader, which starts background threads ...
        try {
            client.connect(timeoutInMilliseconds);
        } catch (TimeoutException e) {
            double seconds = TimeUnit.MILLISECONDS.toSeconds(timeoutInMilliseconds);
            throw new ConnectException("Timed out after " + seconds + " seconds while waiting to connect to the MySQL database at " + host
                    + ":" + port + " with user '" + user + "'", e);
        } catch (AuthenticationException e) {
            throw new ConnectException("Failed to authenticate to the MySQL database at " + host + ":" + port + " with user '" + user + "'",
                    e);
        } catch (Throwable e) {
            throw new ConnectException(
                    "Unable to connect to the MySQL database at " + host + ":" + port + " with user '" + user + "': " + e.getMessage(), e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (events.drainTo(batchEvents, maxBatchSize - batchEvents.size()) == 0 || batchEvents.isEmpty()) {
            // No events to process, so sleep for a bit ...
            sleep(pollIntervalMs);
        }

        // We have at least some records to process ...
        List<SourceRecord> records = new ArrayList<>(batchEvents.size());
        for (Event event : batchEvents) {

            // Update the source offset info ...
            EventHeader eventHeader = event.getHeader();
            EventType eventType = eventHeader.getEventType();
            if (eventType == EventType.ROTATE) {
                EventData eventData = event.getData();
                RotateEventData rotateEventData;
                if (eventData instanceof EventDeserializer.EventDataWrapper) {
                    rotateEventData = (RotateEventData) ((EventDeserializer.EventDataWrapper) eventData).getInternal();
                } else {
                    rotateEventData = (RotateEventData) eventData;
                }
                sourceInfo.setBinlogFilename(rotateEventData.getBinlogFilename());
                sourceInfo.setBinlogPosition(rotateEventData.getBinlogPosition());
                sourceInfo.setRowInEvent(0);
            } else if (eventHeader instanceof EventHeaderV4) {
                EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
                long nextBinlogPosition = trackableEventHeader.getNextPosition();
                if (nextBinlogPosition > 0) {
                    sourceInfo.setBinlogPosition(nextBinlogPosition);
                    sourceInfo.setRowInEvent(0);
                }
            }

            // If there is a handler for this event, forward the event to it ...
            EventHandler handler = eventHandlers.get(eventType);
            if (handler != null) {
                handler.handle(event, sourceInfo, records::add);
            }
        }
        // We've processed them all, so clear the batch and return the records ...
        batchEvents.clear();
        return records;
    }

    @Override
    public void stop() {
        try {
            client.disconnect();
        } catch (IOException e) {
            logger.error("Unexpected error when disconnecting from the MySQL binary log reader", e);
        }
    }

    /**
     * Adds the event into the queue for subsequent batch processing.
     * 
     * @param event the event that was read from the binary log
     */
    protected void enqueue(Event event) {
        if (event != null) events.add(event);
    }

    protected void logEvent(Event event) {
        logger.debug("Received event: " + event);
    }

    protected void sleep(long timeInMillis) {
        try {
            Thread.sleep(timeInMillis);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    protected LifecycleListener traceLifecycleListener() {
        return new LifecycleListener() {
            @Override
            public void onDisconnect(BinaryLogClient client) {
                logger.debug("MySQL Connector disconnected");
            }

            @Override
            public void onConnect(BinaryLogClient client) {
                logger.info("MySQL Connector connected");
            }

            @Override
            public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
                logger.error("MySQL Connector communication failure", ex);
            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
                logger.error("MySQL Connector received event deserialization failure", ex);
            }
        };
    }

    /**
     * The functional interface for all event handler methods.
     */
    @FunctionalInterface
    protected static interface EventHandler {
        void handle(Event event, SourceInfo source, Consumer<SourceRecord> recorder);
    }
}
