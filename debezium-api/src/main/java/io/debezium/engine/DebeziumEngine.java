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
import java.util.function.Predicate;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
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
public interface DebeziumEngine<R> extends Runnable, Closeable {

    String OFFSET_FLUSH_INTERVAL_MS_PROP = "offset.flush.interval.ms";

    /**
     * @return this engine's signaler, if it supports signaling
     * @throws UnsupportedOperationException if signaling is not supported by this engine
     */
    @Incubating
    default Signaler getSignaler() {
        throw new UnsupportedOperationException("Signaling is not supported by this engine");
    }

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

        /**
         * Called after all the tasks have been successfully started and engine has moved to polling phase, but before actual polling is started.
         */
        default void pollingStarted() {
            // nothing by default
        }

        /**
         * Called after all the tasks have successfully exited from the polling loop, i.e. the callback is not called when any of the tasks has thrown
         * exception during polling or was interrupted abruptly.
         */
        default void pollingStopped() {
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
     * A strategy that determines when the engine should automatically shut down based on
     * the records being processed. This is a functional interface extending {@link Predicate}
     * that evaluates each record to decide if shutdown conditions have been met.
     *
     * <p>Shutdown strategies can be configured to evaluate records either before or after
     * they are passed to the consumer, using the {@link ShutdownBuilder} API:
     *
     * <pre>{@code
     * // Shutdown after consuming exactly 100 records
     * engine = DebeziumEngine.create(Connect.class)
     *     .shutdown()
     *     .after()
     *     .consumed()
     *     .records(100)
     *     .notifying(consumer)
     *     .build();
     *
     * // Shutdown before processing records that match a condition
     * engine = DebeziumEngine.create(Connect.class)
     *     .shutdown()
     *     .before()
     *     .consumed()
     *     .custom(record -> record.value().toString().contains("STOP"))
     *     .notifying(consumer)
     *     .build();
     * }</pre>
     *
     * <p><strong>Before vs After:</strong>
     * <ul>
     *   <li><strong>Before:</strong> The strategy evaluates records before they reach the consumer.
     *       If the condition is met, the record triggering shutdown is not processed.</li>
     *   <li><strong>After:</strong> The strategy evaluates records after they have been consumed.
     *       The record triggering shutdown is processed before the engine stops.</li>
     * </ul>
     *
     * <p>When a shutdown strategy's {@link #test(Object)} method returns {@code true},
     * the engine will gracefully stop, allowing
     * the current batch to complete and offsets to be committed before shutdown.
     *
     * @param <T> the type of records to evaluate (typically the same type as consumed by
     *            the engine's consumer)
     *
     * @see ShutdownBuilder
     * @since 3.6.0
     */
    @Incubating
    interface ShutdownStrategy<T> extends Predicate<T> {
    }

    /**
     * A record representing signal sent to the engine via {@link DebeziumEngine.Signaler}.
     * @param id the unique identifier of the signal sent, usually UUID, can be used for deduplication
     * @param type the unique logical name of the code executing the signal
     * @param data  the data in JSON format that are passed to the signal code
     * @param additionalData additional data which might be required by  specific signal types
     */
    @Incubating
    record Signal(String id, String type, String data, Map<String, Object> additionalData) {
    }

    /**
     * Signaler defines the contract for sending signals to connector tasks.
     */
    @Incubating
    interface Signaler {

        /**
         * Send a signal to the connector.
         *
         * @param signal the signal to send
         */
        void signal(Signal signal);
    }

    /**
     * Provides runtime observability into the state of a {@link DebeziumEngine}.
     * This interface allows internal components to monitor whether the engine is
     * actively processing records.
     *
     * @since 3.6.0
     */
    @Incubating
    interface Watcher {

        /**
         * Provides access to engine-level state monitoring.
         *
         * @return the engine watcher; never null
         */
        EngineWatcher engine();

        /**
         * Monitors the consumption state of the engine.
         */
        interface EngineWatcher {

            /**
             * Checks whether the engine is currently in the consuming/polling state,
             * actively reading and processing records from the source.
             *
             * @return {@code true} if the engine is actively consuming records from the
             *         source database; {@code false} if the engine is in any other state
             *         (initializing, starting tasks, stopping, stopped, etc.)
             */
            boolean isConsuming();
        }

    }

    /**
     * builder API for configuring automatic engine shutdown based on record processing.
     * This builder allows you to specify when and how the engine should automatically stop,
     * either before or after consuming a certain number of records or based on custom conditions.
     *
     * <p>The builder follows this chain:
     * <pre>
     * shutdown() → before()/after() → consumed() → records(n)/custom(strategy) → [continue building]
     * </pre>
     *
     * <p><strong>Example - Shutdown after processing 1000 records:</strong>
     * <pre>{@code
     * DebeziumEngine<ChangeEvent<String, String>> engine =
     *     DebeziumEngine.create(Json.class)
     *         .using(props)
     *         .shutdown()
     *             .after()
     *             .consumed()
     *             .records(1000)
     *         .notifying(consumer)
     *         .build();
     * }</pre>
     *
     * <p><strong>Example - Shutdown before processing a sentinel record:</strong>
     * <pre>{@code
     * DebeziumEngine<SourceRecord> engine =
     *     DebeziumEngine.create(Connect.class)
     *         .using(props)
     *         .shutdown()
     *             .before()
     *             .consumed()
     *             .custom(record -> "SHUTDOWN".equals(record.topic()))
     *         .notifying(consumer)
     *         .build();
     * }</pre>
     *
     * @param <R> the record type that will be evaluated for shutdown conditions
     *
     * @see ShutdownStrategy
     * @since 3.6.0
     */
    @Incubating
    interface ShutdownBuilder<R> {

        /**
         * Specifies that the shutdown condition should be evaluated <strong>before</strong>
         * records are passed to the consumer. When the condition is met, the triggering
         * record will not be processed by the consumer.
         *
         * <p>Use this when you want to stop before processing a specific record, such as
         * a sentinel/marker record that signals the end of a data stream.
         *
         * @return the before builder for further configuration; never null
         */
        BeforeBuilder<R> before();

        /**
         * Specifies that the shutdown condition should be evaluated <strong>after</strong>
         * records are passed to the consumer. When the condition is met, the triggering
         * record will have been processed by the consumer before shutdown begins.
         *
         * <p>Use this when you want to ensure a specific number of records have been
         * fully processed, or to stop after processing a specific record.
         *
         * @return the after builder for further configuration; never null
         */
        AfterBuilder<R> after();

        /**
         * Builder for configuring shutdown that occurs before record consumption.
         *
         * @param <R> the record type
         */
        interface BeforeBuilder<R> {

            /**
             * Continues the builder chain to specify the shutdown condition based on
             * consumed records. Currently, this is the only supported condition type.
             *
             * @return the consumed builder for specifying the exact condition; never null
             */
            ConsumedBuilder<R> consuming();
        }

        /**
         * Builder for configuring shutdown that occurs after record consumption.
         *
         * @param <R> the record type
         */
        interface AfterBuilder<R> {
            /**
             * Continues the builder chain to specify the shutdown condition based on
             * consumed records. Currently, this is the only supported condition type.
             *
             * @return the consumed builder for specifying the exact condition; never null
             */
            ConsumedBuilder<R> consuming();

        }

        /**
         * Builder for specifying the actual shutdown condition based on consumed records.
         * Provides two options: a simple record count, or a custom predicate-based strategy.
         *
         * @param <R> the record type
         */
        interface ConsumedBuilder<R> {

            /**
             * Configures the engine to automatically shut down after processing a specific
             * number of records. The engine will count each record as it flows through the
             * pipeline and trigger shutdown when the count is reached.
             *
             *
             * @param number the exact number of records to process before shutdown; must be positive
             * @return the engine builder to continue configuration; never null
             * @throws IllegalArgumentException if number is less than 1
             */
            Builder<R> records(int number);

            /**
             * Configures the engine to automatically shut down based on a custom strategy
             * that evaluates each record. The strategy is a {@link Predicate} that returns
             * {@code true} when shutdown should be triggered.
             *
             * <p>The strategy's {@link ShutdownStrategy#test(Object)} method is called for
             * each record. When it returns {@code true}, the engine will gracefully stop.
             *
             * <p><strong>Example - Stop on a specific value:</strong>
             * <pre>{@code
             * .custom(record -> {
             *     MyValue value = record.value();
             *     return "END_OF_STREAM".equals(value.getMarker());
             * })
             * }</pre>
             *
             * <p><strong>Example - Stop after a time threshold:</strong>
             * <pre>{@code
             * AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
             * .custom(record -> {
             *     return System.currentTimeMillis() - startTime.get() > 60000; // 1 minute
             * })
             * }</pre>
             *
             * @param shutdownStrategy the custom strategy to evaluate records; must not be null
             * @return the engine builder to continue configuration; never null
             * @throws IllegalArgumentException if shutdownStrategy is null
             */
            Builder<R> custom(ShutdownStrategy<R> shutdownStrategy);
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
         * Configures automatic engine shutdown based on record processing conditions.
         * Returns a {@link ShutdownBuilder} that allows you to specify when and how the
         * engine should automatically stop, either before or after consuming a certain
         * number of records or based on custom conditions.
         *
         * <p>This is useful for scenarios such as:
         * <ul>
         *   <li>Processing a finite dataset and stopping after all records are consumed</li>
         *   <li>Running the engine for testing purposes with a limited record count</li>
         *   <li>Stopping when a sentinel/marker record is encountered in the stream</li>
         *   <li>Implementing time-based or condition-based automatic shutdown</li>
         * </ul>
         *
         * <p><strong>Example - Stop after processing exactly 100 records:</strong>
         * <pre>{@code
         * DebeziumEngine<SourceRecord> engine = DebeziumEngine.create(Connect.class)
         *     .using(config)
         *     .shutdown()
         *         .after()
         *         .consumed()
         *         .records(100)
         *     .notifying(record -> {
         *         // Process record
         *     })
         *     .build();
         * }</pre>
         *
         * <p><strong>Example - Stop before processing a shutdown marker:</strong>
         * <pre>{@code
         * DebeziumEngine<ChangeEvent<String, String>> engine =
         *     DebeziumEngine.create(Json.class)
         *         .using(config)
         *         .shutdown()
         *             .before()
         *             .consumed()
         *             .custom(record -> "STOP".equals(record.value()))
         *         .notifying(consumer)
         *         .build();
         * }</pre>
         *
         * <p><strong>Shutdown behavior:</strong>
         * When the shutdown condition is met, the engine will:
         * <ol>
         *   <li>signal graceful shutdown</li>
         *   <li>Allow the current batch of records to complete processing</li>
         *   <li>Commit all offsets to ensure no data loss</li>
         *   <li>Stop all tasks and clean up resources</li>
         *   <li>Call the {@link CompletionCallback} with {@code success=true}</li>
         * </ol>
         *
         * <p><strong>Note:</strong> Shutdown configuration is optional. If not specified,
         * the engine will run indefinitely until explicitly stopped via {@link #close()}
         * or thread interruption.
         *
         * @return a shutdown builder for configuring automatic shutdown conditions; never null
         * @see ShutdownBuilder
         * @see ShutdownStrategy
         * @since 3.6.0
         */
        default ShutdownBuilder<R> shutdown() {
            return null;
        }

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

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link DebeziumEngine} instances.
     * Different formats are used for key, value, and headers of emitted change events.
     * <p>
     * Convenience method, equivalent to calling {@code create(KeyValueChangeEventFormat.of(MyKeyFormat.class, MyValueFormat.class, MyHeaderFormat.class)}.
     *
     * @return the new builder; never null
     */
    static <K, V, H> Builder<ChangeEvent<K, V>> create(Class<? extends SerializationFormat<K>> keyFormat,
                                                       Class<? extends SerializationFormat<V>> valueFormat,
                                                       Class<? extends SerializationFormat<H>> headerFormat) {
        return create(KeyValueHeaderChangeEventFormat.of(keyFormat, valueFormat, headerFormat));
    }

    static <K, V, H> Builder<ChangeEvent<K, V>> create(Class<? extends SerializationFormat<K>> keyFormat,
                                                       Class<? extends SerializationFormat<V>> valueFormat,
                                                       Class<? extends SerializationFormat<H>> headerFormat,
                                                       String builderFactory) {
        return create(KeyValueHeaderChangeEventFormat.of(keyFormat, valueFormat, headerFormat), builderFactory);
    }

    static <S, T, K extends SerializationFormat<S>, V extends SerializationFormat<T>> Builder<ChangeEvent<S, T>> create(KeyValueChangeEventFormat<K, V> format) {
        final BuilderFactory builder = determineBuilderFactory();
        return builder.builder(format);
    }

    static <S, T, U, K extends SerializationFormat<S>, V extends SerializationFormat<T>, H extends SerializationFormat<U>> Builder<ChangeEvent<S, T>> create(KeyValueHeaderChangeEventFormat<K, V, H> format) {
        final BuilderFactory builder = determineBuilderFactory();
        return builder.builder(format);
    }

    static <S, T, U, K extends SerializationFormat<S>, V extends SerializationFormat<T>, H extends SerializationFormat<U>> Builder<ChangeEvent<S, T>> create(KeyValueHeaderChangeEventFormat<K, V, H> format,
                                                                                                                                                             String builderFactory) {
        final BuilderFactory builder = determineBuilderFactory(builderFactory);
        return builder.builder(format);
    }

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link DebeziumEngine} instances.
     * Emitted change events encapsulate both key and value.
     *
     * @return the new builder; never null
     */
    static <T, V extends SerializationFormat<T>> Builder<RecordChangeEvent<T>> create(ChangeEventFormat<V> format) {
        final BuilderFactory builder = determineBuilderFactory();
        return builder.builder(format);
    }

    private static BuilderFactory determineBuilderFactory() {
        return determineBuilderFactory("io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory");
    }

    private static BuilderFactory determineBuilderFactory(String builderFactory) {
        if (builderFactory == null || builderFactory.isBlank()) {
            return determineBuilderFactory();
        }

        final ServiceLoader<BuilderFactory> loader = ServiceLoader.load(BuilderFactory.class);
        final Iterator<BuilderFactory> iterator = loader.iterator();
        if (!iterator.hasNext()) {
            throw new DebeziumException("No implementation of Debezium engine builder was found");
        }
        BuilderFactory builder;
        while (iterator.hasNext()) {
            builder = iterator.next();
            if (builder.getClass().getName().equalsIgnoreCase(builderFactory)) {
                return builder;
            }
        }
        throw new DebeziumException(String.format("No builder factory '%s' found.", builderFactory));
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

        /**
         * Prescribe the output and header formats to be used by the {@link DebeziumEngine}.
         * Usually called by {@link DebeziumEngine#create}.
         * @param format
         * @return this builder object so methods can be chained together; never null
         */
        default <S, T, U, K extends SerializationFormat<S>, V extends SerializationFormat<T>, H extends SerializationFormat<U>> Builder<ChangeEvent<S, T>> builder(KeyValueHeaderChangeEventFormat<K, V, H> format) {
            throw new UnsupportedOperationException("Method must be implemented in order to support headers");
        }
    }
}
