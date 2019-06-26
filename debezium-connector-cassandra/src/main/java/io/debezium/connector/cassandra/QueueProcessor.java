/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.google.common.annotations.VisibleForTesting;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread that constantly polls records from the queue and emit them to Kafka via the KafkaRecordEmitter.
 * The processor is also responsible for marking the offset to file and deleting the commit log files.
 */
public class QueueProcessor extends AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueProcessor.class);
    private static final String NAME = "Change Event Queue Processor";
    private final BlockingEventQueue<Event> blockingEventQueue;
    private final KafkaRecordEmitter kafkaRecordEmitter;
    private final String commitLogRelocationDir;

    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    public QueueProcessor(CassandraConnectorContext context, AtomicBoolean taskState) {
        this(context, taskState, new KafkaRecordEmitter(
                context.getCassandraConnectorConfig().getKafkaConfigs(),
                context.getCassandraConnectorConfig().kafkaTopicPrefix(),
                context.getOffsetWriter(),
                context.getCassandraConnectorConfig().offsetFlushIntervalMs(),
                context.getCassandraConnectorConfig().maxOffsetFlushSize()
        ));
    }

    @VisibleForTesting
    QueueProcessor(CassandraConnectorContext context, AtomicBoolean taskState, KafkaRecordEmitter emitter) {
        super(NAME, taskState);
        this.blockingEventQueue = context.getQueue();
        this.kafkaRecordEmitter = emitter;
        this.commitLogRelocationDir = context.getCassandraConnectorConfig().commitLogRelocationDir();
    }

    @Override
    public void doStart() throws InterruptedException {
        while (isTaskRunning()) {
            processEvents();
        }
    }

    @Override
    public void doStop() throws InterruptedException {
        // drain the queue
        while (!blockingEventQueue.isEmpty()) {
            processEvents();
        }
    }

    @Override
    public void initialize() throws Exception {
        File dir = new File(commitLogRelocationDir);
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                throw new IOException("Failed to create " + commitLogRelocationDir);
            }
        }
        File archiveDir = new File(dir, ARCHIVE_FOLDER);
        if (!archiveDir.exists()) {
            if (!archiveDir.mkdir()) {
                throw new IOException("Failed to create " + archiveDir);
            }
        }
        File errorDir = new File(dir, ERROR_FOLDER);
        if (!errorDir.exists()) {
            if (!errorDir.mkdir()) {
                throw new IOException("Failed to create " + errorDir);
            }
        }
    }

    @Override
    public void destroy() {
        kafkaRecordEmitter.close();
    }

    void processEvents() throws InterruptedException {
        List<Event> events = blockingEventQueue.poll();
        for (Event event : events) {
            processEvent(event);
        }
    }

    private void processEvent(Event event) {
        if (event == null) {
            return;
        }
        switch (event.getEventType()) {
            case CHANGE_EVENT:
                ChangeRecord changeRecord = (ChangeRecord) event;
                kafkaRecordEmitter.emit(changeRecord);
                break;
            case TOMBSTONE_EVENT:
                TombstoneRecord tombstoneRecord = (TombstoneRecord) event;
                kafkaRecordEmitter.emit(tombstoneRecord);
                break;
            case EOF_EVENT:
                EOFEvent eofEvent = (EOFEvent) event;
                String folder = eofEvent.success ? ARCHIVE_FOLDER : ERROR_FOLDER;
                CommitLogUtil.moveCommitLog(eofEvent.file, Paths.get(commitLogRelocationDir, folder));
                break;
            default:
                throw new CassandraConnectorTaskException("Encountered unexpected record with type: " + event.getEventType());
        }
    }
}
