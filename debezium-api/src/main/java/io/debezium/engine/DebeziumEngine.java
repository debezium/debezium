/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine;

import java.io.Closeable;
import java.time.Clock;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;
import io.debezium.engine.spi.OffsetCommitPolicy;

/**
 * A mechanism for running a single Kafka Connect {@link SourceConnector} within an application's process. The engine
 * is entirely standalone and only talks with the source system; no Kafka, Kafka Connect, or Zookeeper processes are needed.
 * Applications using the engine simply set one up and supply a {@link Consumer consumer function} to which the
 * engine will pass all records containing database change events.
 * <p>
 * With the engine, the application that runs the connector assumes all responsibility for fault tolerance,
 * scalability, and durability. Additionally, applications must specify how the engine can store its relational database
 * schema history and offsets. By default, this information will be stored in memory and will thus be lost upon application
 * restart.
 * <p>
 * Engine Is designed to be submitted to an {@link Executor} or {@link ExecutorService} for execution by a single
 * thread, and a running connector can be stopped either by calling {@link #stop()} from another thread or by interrupting
 * the running thread (e.g., as is the case with {@link ExecutorService#shutdownNow()}).
 *
 * @author Randall Hauch
 */
@Incubating
public interface DebeziumEngine<R> extends Runnable, Closeable {

    String OFFSET_FLUSH_INTERVAL_MS_PROP = "offset.flush.interval.ms";

    /**
     * A callback function to be notified when the connector completes.
     */
    interface CompletionCallback {
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
    interface ConnectorCallback {

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
     * Contract passed to {@link ChangeConsumer}s, allowing them to commit single records as they have been processed
     * and to signal that offsets may be flushed eventually.
     */
    interface RecordCommitter<R> {

        /**
         * Marks a single record as processed, must be called for each
         * record.
         *
         * @param record the record to commit
         */
        void markProcessed(R record) throws InterruptedException;

        /**
         * Marks a batch as finished, this may result in committing offsets/flushing
         * data.
         * <p>
         * Should be called when a batch of records is finished being processed.
         */
        void markBatchFinished() throws InterruptedException;

        /**
         * Marks a record with updated source offsets as processed.
         *
         * @param record the record to commit
         * @param sourceOffsets the source offsets to update the record with
         */
        void markProcessed(R record, Offsets sourceOffsets) throws InterruptedException;

        /**
         * Builds a new instance of an object implementing the {@link Offsets} contract.
         *
         * @return the object implementing the {@link Offsets} contract
         */
        Offsets buildOffsets();
    }

    /**
     * Contract that should be passed to {@link RecordCommitter#markProcessed(Object, Offsets)} for marking a record
     * as processed with updated offsets.
     */
    interface Offsets {

        /**
         * Associates a key with a specific value, overwrites the value if the key is already present.
         *
         * @param key key with which to associate the value
         * @param value value to be associated with the key
         */
        void set(String key, Object value);
    }

    /**
     * A contract invoked by the embedded engine when it has received a batch of change records to be processed. Allows
     * to process multiple records in one go, acknowledging their processing once that's done.
     */
    interface ChangeConsumer<R> {

        /**
         * Handles a batch of records, calling the {@link RecordCommitter#markProcessed(Object)}
         * for each record and {@link RecordCommitter#markBatchFinished()} when this batch is finished.
         * @param records the records to be processed
         * @param committer the committer that indicates to the system that we are finished
         */
        void handleBatch(List<R> records, RecordCommitter<R> committer) throws InterruptedException;

        /**
         * Controls whether the change consumer supports processing of tombstone events.
         *
         * @return true if the change consumer supports tombstone events; otherwise false.  The default is true.
         */
        default boolean supportsTombstoneEvents() {
            return true;
        }
    }

    /**
     * A builder to set up and create {@link DebeziumEngine} instances.
     */
    interface Builder<R> {

        /**
         * Call the specified function for every {@link SourceRecord data change event} read from the source database.
         * This method must be called with a non-null consumer.
         *
         * @param consumer the consumer function
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> notifying(Consumer<R> consumer);

        /**
         * Pass a custom ChangeConsumer override the default implementation,
         * this allows for more complex handling of records for batch and async handling
         *
         * @param handler the consumer function
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> notifying(ChangeConsumer<R> handler);

        /**
         * Use the specified configuration for the connector. The configuration is assumed to already be valid.
         *
         * @param config the configuration
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> using(Properties config);

        /**
         * Use the specified class loader to find all necessary classes. Passing <code>null</code> or not calling this method
         * results in the connector using this class's class loader.
         *
         * @param classLoader the class loader
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> using(ClassLoader classLoader);

        /**
         * Use the specified clock when needing to determine the current time. Passing <code>null</code> or not calling this
         * method results in the connector using the {@link Clock#system() system clock}.
         *
         * @param clock the clock
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> using(Clock clock);

        /**
         * When the engine's {@link DebeziumEngine#run()} method completes, call the supplied function with the results.
         *
         * @param completionCallback the callback function; may be null if errors should be written to the log
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> using(CompletionCallback completionCallback);

        /**
         * During the engine's {@link DebeziumEngine#run()} method, call the supplied the supplied function at different
         * stages according to the completion state of each component running within the engine (connectors, tasks etc)
         *
         * @param connectorCallback the callback function; may be null
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> using(ConnectorCallback connectorCallback);

        /**
         * During the engine's {@link DebeziumEngine#run()} method, decide when the offsets
         * should be committed into the {@link OffsetBackingStore}.
         * @param policy
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> using(OffsetCommitPolicy policy);

        /**
         * Build a new connector with the information previously supplied to this builder.
         *
         * @return the embedded connector; never null
         * @throws IllegalArgumentException if a {@link #using(Properties) configuration} or {@link #notifying(Consumer)
         *             consumer function} were not supplied before this method is called
         */
        DebeziumEngine<R> build();
    }

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link DebeziumEngine} instances.
     * The same format is used for key and the value of emitted change events.
     * <p>
     * Convenience method, equivalent to calling {@code create(KeyValueChangeEventFormat.of(MyFormat.class, MyFormat.class)}.
     *
     * @return the new builder; never null
     */
    static <T> Builder<ChangeEvent<T, T>> create(Class<? extends SerializationFormat<T>> format) {
        return create(format, format);
    }

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link DebeziumEngine} instances.
     * Different formats are used for key and the value of emitted change events.
     * <p>
     * Convenience method, equivalent to calling {@code create(KeyValueChangeEventFormat.of(MyKeyFormat.class, MyValueFormat.class)}.
     *
     * @return the new builder; never null
     */
    static <K, V> Builder<ChangeEvent<K, V>> create(Class<? extends SerializationFormat<K>> keyFormat,
                                                    Class<? extends SerializationFormat<V>> valueFormat) {

        return create(KeyValueChangeEventFormat.of(keyFormat, valueFormat));
    }

    static <S, T, K extends SerializationFormat<S>, V extends SerializationFormat<T>> Builder<ChangeEvent<S, T>> create(KeyValueChangeEventFormat<K, V> format) {
        final ServiceLoader<BuilderFactory> loader = ServiceLoader.load(BuilderFactory.class);
        final Iterator<BuilderFactory> iterator = loader.iterator();
        if (!iterator.hasNext()) {
            throw new DebeziumException("No implementation of Debezium engine builder was found");
        }
        final BuilderFactory builder = iterator.next();
        if (iterator.hasNext()) {
            LoggerFactory.getLogger(Builder.class).warn("More than one Debezium engine builder implementation was found, using {}", builder.getClass());
        }
        return builder.builder(format);
    }

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link DebeziumEngine} instances.
     * Emitted change events encapsulate both key and value.
     *
     * @return the new builder; never null
     */
    static <T, V extends SerializationFormat<T>> Builder<RecordChangeEvent<T>> create(ChangeEventFormat<V> format) {
        final ServiceLoader<BuilderFactory> loader = ServiceLoader.load(BuilderFactory.class);
        final Iterator<BuilderFactory> iterator = loader.iterator();
        if (!iterator.hasNext()) {
            throw new DebeziumException("No implementation of Debezium engine builder was found");
        }
        final BuilderFactory builder = iterator.next();
        if (iterator.hasNext()) {
            LoggerFactory.getLogger(Builder.class).warn("More than one Debezium engine builder implementation was found, using {}", builder.getClass());
        }
        return builder.builder(format);
    }

    /**
     * Internal contract between the API and implementation, for bootstrapping the latter.
     * Not intended for direct usage by application code.
     */
    interface BuilderFactory {

        /**
         * Prescribe the output format used by the {@link DebeziumEngine}.
         * Usually called by {@link DebeziumEngine#create}.
         * @param format
         * @return this builder object so methods can be chained together; never null
         */
        <T, V extends SerializationFormat<T>> Builder<RecordChangeEvent<T>> builder(ChangeEventFormat<V> format);

        /**
         * Prescribe the output format used by the {@link DebeziumEngine}.
         * Usually called by {@link DebeziumEngine#create}.
         * @param format
         * @return this builder object so methods can be chained together; never null
         */
        <S, T, K extends SerializationFormat<S>, V extends SerializationFormat<T>> Builder<ChangeEvent<S, T>> builder(KeyValueChangeEventFormat<K, V> format);
    }
}
