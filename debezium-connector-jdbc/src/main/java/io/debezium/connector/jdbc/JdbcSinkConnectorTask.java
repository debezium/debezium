/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.InternalSinkRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectResolver;
import io.debezium.sink.spi.ChangeEventSink;
import io.debezium.util.Stopwatch;
import io.debezium.util.Strings;

/**
 * The main task executing streaming from sink connector.
 * Responsible for lifecycle management of the streaming code.
 *
 * @author Hossein Torabi
 */
public class JdbcSinkConnectorTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorTask.class);

    private static final Class[] EMPTY_CLASS_ARRAY = new Class[0];

    private SessionFactory sessionFactory;

    private enum State {
        RUNNING,
        STOPPED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);
    private final ReentrantLock stateLock = new ReentrantLock();

    private ChangeEventSink changeEventSink;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    private Throwable previousPutException;

    /**
     * There is a change in {@link InternalSinkRecord} API between Connect 3.7 and 3.8.
     * The code now uses 3.8 and use reflection to call old API if new one is not available.
     */
    private boolean usePre380OriginalRecordAccess = false;
    private Method pre380OriginalRecordMethod = null;

    public JdbcSinkConnectorTask() {
        try {
            pre380OriginalRecordMethod = InternalSinkRecord.class.getMethod("originalRecord", EMPTY_CLASS_ARRAY);
            usePre380OriginalRecordAccess = true;
            LOGGER.info("Old InternalSinkRecord class found, will use reflection for calls");
        }
        catch (NoSuchMethodException | SecurityException e) {
            LOGGER.info("New InternalSinkRecord class found");
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        stateLock.lock();

        try {
            if (!state.compareAndSet(State.STOPPED, State.RUNNING)) {
                LOGGER.info("Connector has already been started");
                return;
            }

            // be sure to reset this state
            previousPutException = null;

            final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(props);
            config.validate();

            sessionFactory = config.getHibernateConfiguration().buildSessionFactory();
            StatelessSession session = sessionFactory.openStatelessSession();
            DatabaseDialect databaseDialect = DatabaseDialectResolver.resolve(config, sessionFactory);
            QueryBinderResolver queryBinderResolver = new QueryBinderResolver();
            RecordWriter recordWriter = new RecordWriter(session, queryBinderResolver, config, databaseDialect);

            changeEventSink = new JdbcChangeEventSink(config, session, databaseDialect, recordWriter);
        }
        finally {
            stateLock.unlock();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        Stopwatch putStopWatch = Stopwatch.reusable();
        Stopwatch executeStopWatch = Stopwatch.reusable();
        Stopwatch markProcessedStopWatch = Stopwatch.reusable();
        putStopWatch.start();
        if (previousPutException != null) {
            LOGGER.error("JDBC sink connector failure", previousPutException);
            throw new ConnectException("JDBC sink connector failure", previousPutException);
        }

        LOGGER.debug("Received {} changes.", records.size());

        try {
            executeStopWatch.start();
            changeEventSink.execute(records);
            executeStopWatch.stop();
            markProcessedStopWatch.start();
            records.forEach(this::markProcessed);
            markProcessedStopWatch.stop();
        }
        catch (Throwable throwable) {

            // Capture failure
            LOGGER.error("Failed to process record: {}", throwable.getMessage(), throwable);
            previousPutException = throwable;

            // Stash all records
            records.forEach(this::markNotProcessed);
        }

        putStopWatch.stop();
        LOGGER.trace("[PERF] Total put execution time {}", putStopWatch.durations());
        LOGGER.trace("[PERF] Sink execute execution time {}", executeStopWatch.durations());
        LOGGER.trace("[PERF] Mark processed execution time {}", markProcessedStopWatch.durations());
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        if (LOGGER.isTraceEnabled()) {
            for (TopicPartition partition : partitions) {
                LOGGER.trace("Requested open TopicPartition request for '{}'", partition);
            }
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            if (offsets.containsKey(partition)) {
                LOGGER.trace("Requested close TopicPartition request for '{}'", partition);
                offsets.remove(partition);
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Flush only up to the records processed by this sink
        LOGGER.debug("Flushing offsets: {}", offsets);
        flush(offsets);
        return offsets;
    }

    @Override
    public void stop() {
        stateLock.lock();
        try {

            if (changeEventSink != null) {
                try {
                    changeEventSink.close();

                    if (sessionFactory != null && sessionFactory.isOpen()) {
                        LOGGER.info("Closing the session factory");
                        sessionFactory.close();
                    }
                    else {
                        LOGGER.info("Session factory already closed");
                    }
                }
                catch (Exception e) {
                    LOGGER.error("Failed to gracefully close resources.", e);
                }
            }
        }
        finally {
            if (previousPutException != null) {
                previousPutException = null;
            }
            if (changeEventSink != null) {
                changeEventSink = null;
            }
            stateLock.unlock();
        }
    }

    /**
     * Marks a sink record as processed.
     *
     * @param record sink record, should not be {@code null}
     */
    private void markProcessed(SinkRecord record) {
        final String topicName = getOriginalTopicName(record);
        if (Strings.isNullOrBlank(topicName)) {
            return;
        }

        LOGGER.trace("Marking processed record for topic {}", topicName);

        final long kafkaOffset = getOriginalKafkaOffset(record);
        final TopicPartition topicPartition = new TopicPartition(topicName, getOriginalKafkaPartition(record));
        final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(kafkaOffset + 1L);

        final OffsetAndMetadata existing = offsets.put(topicPartition, offsetAndMetadata);
        if (existing == null) {
            LOGGER.trace("Advanced topic {} to offset {}.", topicName, kafkaOffset);
        }
        else {
            LOGGER.trace("Updated topic {} from offset {} to {}.", topicName, existing.offset(), kafkaOffset);
        }
    }

    /**
     * Marks a single record as not processed.
     *
     * @param record sink record, should not be {@code null}
     */
    private void markNotProcessed(SinkRecord record) {
        // Sink connectors operate on batches and a batch could technically include a stream of records
        // where the same topic/partition tuple exists in the batch at various points, before and after
        // other topic/partition tuples. When marking a record as not processed, we are only interested
        // in doing this if this tuple is not already in the map as a previous entry could have been
        // added because an earlier record was either processed or marked as not processed since any
        // remaining entries in the batch call this method on failures.
        final String topicName = getOriginalTopicName(record);
        final Integer kafkaPartition = getOriginalKafkaPartition(record);
        final long kafkaOffset = getOriginalKafkaOffset(record);

        final TopicPartition topicPartition = new TopicPartition(topicName, kafkaPartition);
        if (!offsets.containsKey(topicPartition)) {
            LOGGER.debug("Rewinding topic {} offset to {}.", topicName, kafkaOffset);
            final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(kafkaOffset);
            offsets.put(topicPartition, offsetAndMetadata);
        }
    }

    private String getOriginalTopicName(SinkRecord record) {
        // DBZ-6491
        // This is a current workaround to resolve the original topic name from the broker as
        // the value in the SinkRecord#topic may have been mutated with SMTs and would no
        // longer represent the logical topic name on the broker.
        //
        // I intend to see whether the Kafka team could expose this original value on the
        // SinkRecord contract for scenarios such as this to avoid the need to depend on
        // connect-runtime. If so, we can drop that dependency, but since this is only a
        // Kafka Connect implementation at this point, it's a fair workaround.
        //
        if (record instanceof InternalSinkRecord) {
            if (usePre380OriginalRecordAccess) {
                try {
                    return ((ConsumerRecord<byte[], byte[]>) pre380OriginalRecordMethod.invoke(record, EMPTY_CLASS_ARRAY)).topic();
                }
                catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new DebeziumException("Failed to access original record data", e);
                }
            }
            return ((InternalSinkRecord) record).context().original().topic();
        }
        return null;
    }

    private Integer getOriginalKafkaPartition(SinkRecord record) {
        try {
            // Added in Kafka 3.6 and should be used as it will contain the details pre-transformations
            return record.originalKafkaPartition();
        }
        catch (NoSuchMethodError e) {
            // Fallback to old method for Kafka 3.5 or earlier
            return record.kafkaPartition();
        }
    }

    private long getOriginalKafkaOffset(SinkRecord record) {
        try {
            // Added in Kafka 3.6 and should be used as it will contain the details pre-transformations
            return record.originalKafkaOffset();
        }
        catch (NoSuchMethodError e) {
            // Fallback to old method for Kafka 3.5 or earlier
            return record.kafkaOffset();
        }
    }
}
