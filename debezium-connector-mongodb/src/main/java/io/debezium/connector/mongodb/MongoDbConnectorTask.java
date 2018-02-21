/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.util.LoggingContext;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.Threads;

/**
 * A Kafka Connect source task that replicates the changes from one or more MongoDB replica sets, using one {@link Replicator}
 * for each replica set.
 * <p>
 * Generally, the {@link MongoDbConnector} assigns each replica set to a separate task, although multiple
 * replica sets will be assigned to each task when the maximum number of tasks is limited. Regardless, every task will use a
 * separate thread to replicate the contents of each replica set, and each replication thread may use multiple threads
 * to perform an initial sync of the replica set.
 *
 * @see MongoDbConnector
 * @see MongoDbConnectorConfig
 * @author Randall Hauch
 */
@ThreadSafe
public final class MongoDbConnectorTask extends BaseSourceTask {

    private static final String CONTEXT_NAME = "mongodb-connector-task";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Deque<Replicator> replicators = new ConcurrentLinkedDeque<>();
    private final RecordBatchSummarizer recordSummarizer = new RecordBatchSummarizer();

    // These are all effectively constants between start(...) and stop(...)
    private volatile ChangeEventQueue<SourceRecord> queue;
    private volatile String taskName;
    private volatile MongoDbTaskContext taskContext;
    private volatile Throwable replicatorError;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Configuration config) {
        if (!this.running.compareAndSet(false, true)) {
            // Already running ...
            return;
        }

        // Read the configuration and set up the replication context ...
        this.taskName = "task" + config.getInteger(MongoDbConnectorConfig.TASK_ID);
        final MongoDbTaskContext taskContext = new MongoDbTaskContext(config);
        this.taskContext = taskContext;
        PreviousContext previousLogContext = taskContext.configureLoggingContext(taskName);

        try {
            // Read from the configuration the information about the replica sets we are to watch ...
            final String hosts = config.getString(MongoDbConnectorConfig.HOSTS);
            final ReplicaSets replicaSets = ReplicaSets.parse(hosts);
            if ( replicaSets.validReplicaSetCount() == 0) {
                throw new ConnectException(
                        "Unable to start MongoDB connector task since no replica sets were found at " + hosts);
            }

            MongoDbConnectorConfig connectorConfig = new MongoDbConnectorConfig(config);

            // Set up the task record queue ...
            this.queue = new ChangeEventQueue.Builder<SourceRecord>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .loggingContextSupplier(this::getLoggingContext)
                    .build();

            // Get the offsets for each of replica set partition ...
            SourceInfo source = taskContext.source();
            Collection<Map<String, String>> partitions = new ArrayList<>();
            replicaSets.onEachReplicaSet(replicaSet -> {
                String replicaSetName = replicaSet.replicaSetName(); // may be null for standalone servers
                if (replicaSetName != null) {
                    partitions.add(source.partition(replicaSetName));
                }
            });
            context.offsetStorageReader().offsets(partitions).forEach(source::setOffsetFor);

            // Set up a replicator for each replica set ...
            final int numThreads = replicaSets.replicaSetCount();
            final ExecutorService executor = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), "replicator", numThreads);
            AtomicInteger stillRunning = new AtomicInteger(numThreads);
            logger.info("Ignoring unnamed replica sets: {}", replicaSets.unnamedReplicaSets());
            logger.info("Starting {} thread(s) to replicate replica sets: {}", numThreads, replicaSets);
            replicaSets.validReplicaSets().forEach(replicaSet -> {
                // Create a replicator for this replica set ...
                Replicator replicator = new Replicator(taskContext, replicaSet, queue::enqueue, this::failedReplicator);
                replicators.add(replicator);
                // and submit it for execution ...
                executor.submit(() -> {
                    try {
                        // Configure the logging to use the replica set name ...
                        taskContext.configureLoggingContext(replicaSet.replicaSetName());
                        // Run the replicator, which should run forever until it is stopped ...
                        replicator.run();
                    } finally {
                        try {
                            replicators.remove(replicator);
                        } finally {
                            if (stillRunning.decrementAndGet() == 0) {
                                // we are the last one, so clean up ...
                                try {
                                    executor.shutdown();
                                } finally {
                                    taskContext.getConnectionContext().shutdown();
                                }
                            }
                        }
                    }
                });
            });
            logger.info("Successfully started MongoDB connector task with {} thread(s) for replica sets {}", numThreads, replicaSets);
        } finally {
            previousLogContext.restore();
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (replicatorError != null) {
            throw new ConnectException("Failing connector task, at least one of the replicators has failed");
        }
        List<SourceRecord> records = queue.poll();
        recordSummarizer.accept(records);
        return records;
    }

    @Override
    public void stop() {
        PreviousContext previousLogContext = this.taskContext.configureLoggingContext(taskName);
        try {
            // Signal to the 'poll()' method that it should stop what its doing ...
            if (this.running.compareAndSet(true, false)) {
                logger.info("Stopping MongoDB task");
                // Stop all running replicators ...
                Replicator replicator = null;
                int counter = 0;
                while ((replicator = this.replicators.poll()) != null) {
                    replicator.stop();
                    ++counter;
                }
                logger.info("Stopped MongoDB replication task by stopping {} replicator threads", counter);
            }
        } catch (Throwable e) {
            logger.error("Unexpected error shutting down the MongoDB replication task", e);
        } finally {
            previousLogContext.restore();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MongoDbConnectorConfig.ALL_FIELDS;
    }

    private LoggingContext.PreviousContext getLoggingContext() {
        return taskContext.configureLoggingContext(CONTEXT_NAME);
    }

    private void failedReplicator(Throwable t) {
        replicatorError = t;
        stop();
    }

    protected final class RecordBatchSummarizer implements Consumer<List<SourceRecord>> {
        private final Map<String, ReplicaSetSummary> summaryByReplicaSet = new HashMap<>();

        @Override
        public void accept(List<SourceRecord> records) {
            if (records.isEmpty()) return;
            if (!logger.isInfoEnabled()) return;
            summaryByReplicaSet.clear();
            records.forEach(record -> {
                String replicaSetName = SourceInfo.replicaSetNameForPartition(record.sourcePartition());
                if (replicaSetName != null) {
                    summaryByReplicaSet.computeIfAbsent(replicaSetName, rsName -> new ReplicaSetSummary()).add(record);
                }
            });
            if (!summaryByReplicaSet.isEmpty()) {
                PreviousContext prevContext = taskContext.configureLoggingContext("task");
                try {
                    summaryByReplicaSet.forEach((rsName, summary) -> {
                        logger.info("{} records sent for replica set '{}', last offset: {}",
                                    summary.recordCount(), rsName, summary.lastOffset());
                    });
                } finally {
                    prevContext.restore();
                }
            }
        }
    }

    protected static final class ReplicaSetSummary {
        private int numRecords = 0;
        private Map<String, ?> lastOffset;

        public void add(SourceRecord record) {
            ++numRecords;
            lastOffset = record.sourceOffset();
        }

        public int recordCount() {
            return numRecords;
        }

        public Map<String, ?> lastOffset() {
            return lastOffset;
        }
    }
}
