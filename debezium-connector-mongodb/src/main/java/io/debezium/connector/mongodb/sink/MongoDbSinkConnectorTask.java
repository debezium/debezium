/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.internal.VisibleForTesting;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.connection.MongoDbConnectionContext;
import io.debezium.dlq.ErrorReporter;
import io.debezium.sink.DebeziumSinkRecord;

public class MongoDbSinkConnectorTask extends SinkTask {
    static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSinkConnectorTask.class);
    private static final String CONNECTOR_TYPE = "sink";
    private MongoDbChangeEventSink mongoSink;

    @Override
    public String version() {
        return Module.version();
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     *
     * @param props initial configuration
     */
    @SuppressWarnings("try")
    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("Starting MongoDB sink task");
        var config = Configuration.from(props);
        final MongoDbSinkConnectorConfig sinkConfig = new MongoDbSinkConnectorConfig(config);
        MongoClient client = null;
        try {
            MongoDbConnectionContext connectionContext = new MongoDbConnectionContext(config);
            client = connectionContext.getMongoClient();
            mongoSink = new MongoDbChangeEventSink(sinkConfig, client, createErrorReporter());
        }
        catch (RuntimeException taskStartingException) {
            // noinspection EmptyTryBlock
            try (MongoClient autoCloseableClient = client) {
                // just using try-with-resources to ensure they all get closed, even in the case of
                // exceptions
            }
            catch (RuntimeException resourceReleasingException) {
                taskStartingException.addSuppressed(resourceReleasingException);
            }
            throw new ConnectException("Failed to start MongoDB sink task", taskStartingException);
        }
        LOGGER.debug("Started MongoDB sink task");
    }

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     *
     * <p>If this operation fails, the SinkTask may throw a {@link
     * org.apache.kafka.connect.errors.RetriableException} to indicate that the framework should
     * attempt to retry the same call again. Other exceptions will cause the task to be stopped
     * immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before
     * the batch will be retried.
     *
     * @param records the set of records to send
     */
    @Override
    public void put(final Collection<SinkRecord> records) {
        mongoSink.execute(records);
    }

    /**
     * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
     *
     * @param currentOffsets the current offset state as of the last call to {@link
     *     #put(Collection)}}, provided for convenience but could also be determined by tracking all
     *     offsets included in the {@link SinkRecord}s passed to {@link #put}.
     */
    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // NOTE: flush is not used for now...
        LOGGER.debug("Flush called - noop");
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once
     * outstanding calls to other methods have completed (e.g., {@link #put(Collection)} has returned)
     * and a final {@link #flush(Map)} and offset commit has completed. Implementations of this method
     * should only need to perform final cleanup operations, such as closing network connections to
     * the sink system.
     */
    @Override
    public void stop() {
        LOGGER.info("Stopping MongoDB sink task");
        if (mongoSink != null) {
            mongoSink.close();
        }
    }

    private ErrorReporter createErrorReporter() {
        ErrorReporter result = nopErrorReporter();
        if (context != null) {
            try {
                ErrantRecordReporter errantRecordReporter = context.errantRecordReporter();
                if (errantRecordReporter != null) {
                    result = (DebeziumSinkRecord record, Exception e) -> {
                        if (record instanceof KafkaDebeziumSinkRecord kafkaRecord) {
                            errantRecordReporter.report(kafkaRecord.getOriginalKafkaRecord(), e);
                        }
                    };
                }
                else {
                    LOGGER.info("Errant record reporter not configured.");
                }
            }
            catch (NoClassDefFoundError | NoSuchMethodError e) {
                // Will occur in Connect runtimes earlier than 2.6
                LOGGER.info("Kafka versions prior to 2.6 do not support the errant record reporter.");
            }
        }
        return result;
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    static ErrorReporter nopErrorReporter() {
        return (record, e) -> {
            /* do nothing */ };
    }
}
