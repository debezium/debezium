/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.embedded.internal.BatchSendingEmbeddedEngine;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;

/**
 * A mechanism for running a single Kafka Connect {@link SourceConnector} within an application's process. An embedded connector
 * is entirely standalone and only talks with the source system; no Kafka, Kafka Connect, or Zookeeper processes are needed.
 * Applications using an embedded connector simply set one up and supply a {@link Consumer consumer function} to which the
 * connector will pass all {@link SourceRecord}s containing database change events.
 * <p>
 * With an embedded connector, the application that runs the connector assumes all responsibility for fault tolerance,
 * scalability, and durability. Additionally, applications must specify how the connector can store its relational database
 * schema history and offsets. By default, this information will be stored in memory and will thus be lost upon application
 * restart.
 * <p>
 * Embedded connectors are designed to be submitted to an {@link Executor} or {@link ExecutorService} for execution by a single
 * thread, and a running connector can be stopped either by calling {@link #stop()} from another thread or by interrupting
 * the running thread (e.g., as is the case with {@link ExecutorService#shutdownNow()}).
 *
 * @author Randall Hauch
 */
@ThreadSafe
public interface EmbeddedEngine extends Runnable {

    /**
     * A required field for an embedded connector that specifies the unique name for the connector instance.
     */
    public static final Field ENGINE_NAME = Field.create("name")
                                                 .withDescription("Unique name for this connector instance.")
                                                 .withValidation(Field::isRequired);

    /**
     * A required field for an embedded connector that specifies the name of the normal Debezium connector's Java class.
     */
    public static final Field CONNECTOR_CLASS = Field.create("connector.class")
                                                     .withDescription("The Java class for the connector")
                                                     .withValidation(Field::isRequired);

    /**
     * An optional field that specifies the name of the class that implements the {@link OffsetBackingStore} interface,
     * and that will be used to store offsets recorded by the connector.
     */
    public static final Field OFFSET_STORAGE = Field.create("offset.storage")
                                                    .withDescription("The Java class that implements the `OffsetBackingStore` "
                                                            + "interface, used to periodically store offsets so that, upon "
                                                            + "restart, the connector can resume where it last left off.")
                                                    .withDefault(FileOffsetBackingStore.class.getName());

    /**
     * An optional field that specifies the file location for the {@link FileOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_FILE_FILENAME = Field.create(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG)
                                                                  .withDescription("The file where offsets are to be stored. Required when "
                                                                          + "'offset.storage' is set to the " +
                                                                          FileOffsetBackingStore.class.getName() + " class.")
                                                                  .withDefault("");

    /**
     * An optional field that specifies the topic name for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_TOPIC = Field.create(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG)
                                                                .withDescription("The name of the Kafka topic where offsets are to be stored. "
                                                                        + "Required with other properties when 'offset.storage' is set to the "
                                                                        + KafkaOffsetBackingStore.class.getName() + " class.")
                                                                .withDefault("");

    /**
     * An optional field that specifies the number of partitions for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_PARTITIONS = Field.create(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG)
                                                                     .withType(ConfigDef.Type.INT)
                                                                     .withDescription("The number of partitions used when creating the offset storage topic. "
                                                                             + "Required with other properties when 'offset.storage' is set to the "
                                                                             + KafkaOffsetBackingStore.class.getName() + " class.");

    /**
     * An optional field that specifies the replication factor for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR = Field.create(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
                                                                             .withType(ConfigDef.Type.SHORT)
                                                                             .withDescription("Replication factor used when creating the offset storage topic. "
                                                                                     + "Required with other properties when 'offset.storage' is set to the "
                                                                                     + KafkaOffsetBackingStore.class.getName() + " class.");

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    public static final Field OFFSET_FLUSH_INTERVAL_MS = Field.create("offset.flush.interval.ms")
                                                              .withDescription("Interval at which to try committing offsets. The default is 1 minute.")
                                                              .withDefault(60000L)
                                                              .withValidation(Field::isNonNegativeInteger);

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    public static final Field OFFSET_COMMIT_TIMEOUT_MS = Field.create("offset.flush.timeout.ms")
                                                              .withDescription("Maximum number of milliseconds to wait for records to flush and partition offset data to be"
                                                                      + " committed to offset storage before cancelling the process and restoring the offset "
                                                                      + "data to be committed in a future attempt.")
                                                              .withDefault(5000L)
                                                              .withValidation(Field::isPositiveInteger);

    public static final Field OFFSET_COMMIT_POLICY = Field.create("offset.commit.policy")
                                                          .withDescription("The fully-qualified class name of the commit policy type. This class must implement the interface "
                                                                      + OffsetCommitPolicy.class.getName()
                                                                      + ". The default is a periodic commity policy based upon time intervals.")
                                                          .withDefault(OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class.getName())
                                                          .withValidation(Field::isClassName);

    /**
     * A callback function to be notified when the connector completes.
     */
    public interface CompletionCallback {
        /**
         * Handle the completion of the embedded connector engine.
         *
         * @param success {@code true} if the connector completed normally, or {@code false} if the connector produced an error
         *            that prevented startup or premature termination.
         * @param message the completion message; never null
         * @param error the error, or null if there was no exception
         */
        void handle(boolean success, String message, Throwable error);
    }

    /**
     * Callback function which informs users about the various stages a connector goes through during startup
     */
    public interface ConnectorCallback {

        /**
         * Called after a connector has been successfully started by the engine; i.e. {@link SourceConnector#start(Map)} has
         * completed successfully
         */
        default void connectorStarted() {
            // nothing by default
        }

        /**
         * Called after a connector has been successfully stopped by the engine; i.e. {@link SourceConnector#stop()} has
         * completed successfully
         */
        default void connectorStopped() {
            // nothing by default
        }

        /**
         * Called after a connector task has been successfully started by the engine; i.e. {@link SourceTask#start(Map)} has
         * completed successfully
         */
        default void taskStarted() {
            // nothing by default
        }

        /**
         * Called after a connector task has been successfully stopped by the engine; i.e. {@link SourceTask#stop()} has
         * completed successfully
         */
        default void taskStopped() {
            // nothing by default
        }
    }

    /**
     * A callback function to be notified when the connector completes.
     */
    public static class CompletionResult implements CompletionCallback {
        private final CompletionCallback delegate;
        private final CountDownLatch completed = new CountDownLatch(1);
        private boolean success;
        private String message;
        private Throwable error;

        public CompletionResult() {
            this(null);
        }

        public CompletionResult(CompletionCallback delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(boolean success, String message, Throwable error) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.completed.countDown();
            if (delegate != null) {
                delegate.handle(success, message, error);
            }
        }

        /**
         * Causes the current thread to wait until the {@link #handle(boolean, String, Throwable) completion occurs}
         * or until the thread is {@linkplain Thread#interrupt interrupted}.
         * <p>
         * This method returns immediately if the connector has completed already.
         *
         * @throws InterruptedException if the current thread is interrupted while waiting
         */
        public void await() throws InterruptedException {
            this.completed.await();
        }

        /**
         * Causes the current thread to wait until the {@link #handle(boolean, String, Throwable) completion occurs},
         * unless the thread is {@linkplain Thread#interrupt interrupted}, or the specified waiting time elapses.
         * <p>
         * This method returns immediately if the connector has completed already.
         *
         * @param timeout the maximum time to wait
         * @param unit the time unit of the {@code timeout} argument
         * @return {@code true} if the completion was received, or {@code false} if the waiting time elapsed before the completion
         *         was received.
         * @throws InterruptedException if the current thread is interrupted while waiting
         */
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return this.completed.await(timeout, unit);
        }

        /**
         * Determine if the connector has completed.
         *
         * @return {@code true} if the connector has completed, or {@code false} if the connector is still running and this
         *         callback has not yet been {@link #handle(boolean, String, Throwable) notified}
         */
        public boolean hasCompleted() {
            return completed.getCount() == 0;
        }

        /**
         * Get whether the connector completed normally.
         *
         * @return {@code true} if the connector completed normally, or {@code false} if the connector produced an error that
         *         prevented startup or premature termination (or the connector has not yet {@link #hasCompleted() completed})
         */
        public boolean success() {
            return success;
        }

        /**
         * Get the completion message.
         *
         * @return the completion message, or null if the connector has not yet {@link #hasCompleted() completed}
         */
        public String message() {
            return message;
        }

        /**
         * Get the completion error, if there is one.
         *
         * @return the completion error, or null if there is no error or connector has not yet {@link #hasCompleted() completed}
         */
        public Throwable error() {
            return error;
        }

        /**
         * Determine if there is a completion error.
         *
         * @return {@code true} if there is a {@link #error completion error}, or {@code false} if there is no error or
         *         the connector has not yet {@link #hasCompleted() completed}
         */
        public boolean hasError() {
            return error != null;
        }
    }

    /**
     * A builder to set up and create {@link EmbeddedEngine} instances.
     */
    public static interface Builder {

        /**
         * Call the specified function for every {@link SourceRecord data change event} read from the source database.
         * This method must be called with a non-null consumer.
         *
         * @param consumer the consumer function
         * @return this builder object so methods can be chained together; never null
         */
        Builder notifying(Consumer<SourceRecord> consumer);

        /**
         * Use the specified configuration for the connector. The configuration is assumed to already be valid.
         *
         * @param config the configuration
         * @return this builder object so methods can be chained together; never null
         */
        Builder using(Configuration config);

        /**
         * Use the specified class loader to find all necessary classes. Passing <code>null</code> or not calling this method
         * results in the connector using this class's class loader.
         *
         * @param classLoader the class loader
         * @return this builder object so methods can be chained together; never null
         */
        Builder using(ClassLoader classLoader);

        /**
         * Use the specified clock when needing to determine the current time. Passing <code>null</code> or not calling this
         * method results in the connector using the {@link Clock#system() system clock}.
         *
         * @param clock the clock
         * @return this builder object so methods can be chained together; never null
         */
        Builder using(Clock clock);

        /**
         * When the engine's {@link EmbeddedEngine#run()} method completes, call the supplied function with the results.
         *
         * @param completionCallback the callback function; may be null if errors should be written to the log
         * @return this builder object so methods can be chained together; never null
         */
        Builder using(CompletionCallback completionCallback);

        /**
         * During the engine's {@link EmbeddedEngine#run()} method, call the supplied the supplied function at different
         * stages according to the completion state of each component running within the engine (connectors, tasks etc)
         *
         * @param connectorCallback the callback function; may be null
         * @return this builder object so methods can be chained together; never null
         */
        Builder using(ConnectorCallback connectorCallback);

        /**
         * During the engine's {@link EmbeddedEngine#run()} method, decide when the offsets
         * should be committed into the {@link OffsetBackingStore}.
         * @param policy
         * @return this builder object so methods can be chained together; never null
         */
        Builder using(OffsetCommitPolicy policy);

        /**
         * Build a new connector with the information previously supplied to this builder.
         *
         * @return the embedded connector; never null
         * @throws IllegalArgumentException if a {@link #using(Configuration) configuration} or {@link #notifying(Consumer)
         *             consumer function} were not supplied before this method is called
         */
        EmbeddedEngine build();
    }

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link EmbeddedEngine} instances.
     *
     * @return the new builder; never null
     */
    public static Builder create() {
        return new Builder() {
            private Configuration config;
            private Consumer<SourceRecord> consumer;
            private ClassLoader classLoader;
            private Clock clock;
            private CompletionCallback completionCallback;
            private ConnectorCallback connectorCallback;
            private OffsetCommitPolicy offsetCommitPolicy = null;

            @Override
            public Builder using(Configuration config) {
                this.config = config;
                return this;
            }

            @Override
            public Builder using(ClassLoader classLoader) {
                this.classLoader = classLoader;
                return this;
            }

            @Override
            public Builder using(Clock clock) {
                this.clock = clock;
                return this;
            }

            @Override
            public Builder using(CompletionCallback completionCallback) {
                this.completionCallback = completionCallback;
                return this;
            }

            @Override
            public Builder using(ConnectorCallback connectorCallback) {
                this.connectorCallback = connectorCallback;
                return this;
            }

            @Override
            public Builder using(OffsetCommitPolicy offsetCommitPolicy) {
                this.offsetCommitPolicy = offsetCommitPolicy;
                return this;
            }

            @Override
            public Builder notifying(Consumer<SourceRecord> consumer) {
                this.consumer = consumer;
                return this;
            }

            @Override
            public EmbeddedEngine build() {
                if (classLoader == null) classLoader = getClass().getClassLoader();
                if (clock == null) clock = Clock.system();
                Objects.requireNonNull(config, "A connector configuration must be specified.");
                Objects.requireNonNull(consumer, "A connector consumer must be specified.");
                return new BatchSendingEmbeddedEngine(config, classLoader, clock,
                        consumer, completionCallback, connectorCallback, offsetCommitPolicy);
            }

        };
    }

    /**
     * Wait for the connector to complete processing. If the processor is not running, this method returns immediately; however,
     * if the processor is {@link #stop() stopped} and restarted before this method is called, this method will return only
     * when it completes the second time.
     *
     * @param timeout the maximum amount of time to wait before returning
     * @param unit the unit of time; may not be null
     * @return {@code true} if the connector completed within the timeout (or was not running), or {@code false} if it is still
     *         running when the timeout occurred
     * @throws InterruptedException if this thread is interrupted while waiting for the completion of the connector
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Stop the execution of this embedded connector. This method does not block until the connector is stopped; use
     * {@link #await(long, TimeUnit)} for this purpose.
     *
     * @return {@code true} if the connector was {@link #run() running} and will eventually stop, or {@code false} if it was not
     *         running when this method is called
     * @see #await(long, TimeUnit)
     */
    boolean stop();

    void run();

    /**
     * Determine if this embedded connector is currently running.
     *
     * @return {@code true} if running, or {@code false} otherwise
     */
    boolean isRunning();
}
