/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB changes. 
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorTask extends SourceTask {
    
    private static final String CONTEXT_NAME = "postgres-connector-task";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    private PostgresTaskContext taskContext;
    private BlockingQueue<SourceRecord> queue;
    private int maxBatchSize;
    private RecordsProducer producer;
    private Metronome metronome;
    
    public PostgresConnectorTask() {
    }
    
    @Override
    public void start(Map<String, String> props) {
        if (running.get()) {
            // already running
            return;
        }
    
        if (context == null) {
            throw new ConnectException("Unexpected null context");
        }
    
        // Validate the configuration ...
        PostgresConnectorConfig config = new PostgresConnectorConfig(Configuration.from(props));
        if (!config.validateAndRecord(logger::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        
        
        // create the task context and schema...
        PostgresSchema schema = new PostgresSchema(config);
        this.taskContext = new PostgresTaskContext(config, schema);
        
        // create the queue in which records will be produced
        this.queue = new LinkedBlockingDeque<>(config.maxQueueSize());
        this.maxBatchSize = config.maxBatchSize();
        
        SourceInfo sourceInfo = new SourceInfo(config.serverName());
        Map<String, Object> existingOffset = context.offsetStorageReader().offset(sourceInfo.partition());
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            //Print out the server information
            try (PostgresConnection connection = taskContext.createConnection()) {
                logger.info(connection.serverInfo().toString());
            }
            
            if (existingOffset == null) {
                logger.info("No previous offset found");
                if (config.snapshotNeverAllowed()) {
                    logger.info("Snapshots are not allowed as per configuration, starting streaming logical changes only");
                    producer = new RecordsStreamProducer(taskContext, sourceInfo);
                } else {
                    // otherwise we always want to take a snapshot at startup
                    createSnapshotProducer(taskContext, sourceInfo, config.initialOnlySnapshot());
                }
            } else {
                sourceInfo.load(existingOffset);
                logger.info("Found previous offset {}", sourceInfo);
                if (sourceInfo.isSnapshotInEffect()) {
                    if (config.snapshotNeverAllowed()) {
                        // No snapshots are allowed
                        String msg = "The connector previously stopped while taking a snapshot, but now the connector is configured "
                                     + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.";
                        throw new ConnectException(msg);
                    } else {
                        logger.info("Found previous incomplete snapshot");
                        createSnapshotProducer(taskContext, sourceInfo, config.initialOnlySnapshot());
                    }
                } else if (config.alwaysTakeSnapshot()) {
                    logger.info("Taking a new snapshot as per configuration");
                    producer = new RecordsSnapshotProducer(taskContext, sourceInfo, true);
                } else {
                    logger.info(
                            "Previous snapshot has completed successfully, streaming logical changes from last known position");
                    producer = new RecordsStreamProducer(taskContext, sourceInfo);
                }
            }
    
            metronome = Metronome.sleeper(config.pollIntervalMs(), TimeUnit.MILLISECONDS, Clock.SYSTEM);
            producer.start(this::enqueueRecord);
            running.compareAndSet(false, true);
        }  catch (SQLException e) {
            throw new ConnectException(e);
        } finally {
            previousContext.restore();
        }
    }
    
    private void enqueueRecord(SourceRecord record) {
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            queue.put(record);    
            if (logger.isDebugEnabled()) {
                logger.debug("Placed source record '{}' into queue", record);
            }
        } catch (InterruptedException e) {
            logger.debug("received interrupt request");
            // clear the interrupted status
            Thread.interrupted();
        } finally {
            previousContext.restore();
        }
    }
    
    private void createSnapshotProducer(PostgresTaskContext taskContext, SourceInfo sourceInfo, boolean initialOnlySnapshot) {
        if (initialOnlySnapshot) {
            logger.info("Taking only a snapshot of the DB without streaming any changes afterwards...");
            producer = new RecordsSnapshotProducer(taskContext, sourceInfo, false); 
        } else {
            logger.info("Taking a new snapshot of the DB and streaming logical changes once the snapshot is finished...");
            producer = new RecordsSnapshotProducer(taskContext, sourceInfo, true);   
        }
    }
    
    @Override
    public void commit() throws InterruptedException {
        if (running.get()) {
            producer.commit();
        }
    }
    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            logger.debug("polling records...");
            List<SourceRecord> records = new ArrayList<>();
            while (running.get() && queue.drainTo(records, maxBatchSize) == 0) {
                if (taskContext.getTaskFailure() != null) {
                    throw new ConnectException(taskContext.getTaskFailure());
                }
                try {
                    logger.debug("no records available yet, sleeping a bit...");
                    // no records yet, so wait a bit
                    metronome.pause();
                    logger.debug("checking for more records...");
                } catch (InterruptedException e) {
                    // we've been requested to stop polling
                    Thread.interrupted();
                    break;
                }
            }
            return records;
        } finally {
            previousContext.restore();    
        }
    }
    
    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            producer.stop();
        }
    }
    
    @Override
    public String version() {
        return Module.version();
    }
}
