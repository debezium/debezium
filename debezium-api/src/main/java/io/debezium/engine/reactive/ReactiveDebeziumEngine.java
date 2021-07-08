/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.reactive;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;
import io.debezium.engine.spi.OffsetCommitPolicy;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Clock;
import java.util.*;
import java.util.function.Consumer;

@Incubating
public interface ReactiveDebeziumEngine<R> extends Closeable {
    public static final String OFFSET_FLUSH_INTERVAL_MS_PROP = "offset.flush.interval.ms";

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
     * A builder to set up and create {@link ReactiveDebeziumEngine} instances.
     */
    public static interface Builder<R> {
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
         * When the engine's {@link ReactiveDebeziumEngine#run()} method completes, call the supplied function with the results.
         *
         * @param completionCallback the callback function; may be null if errors should be written to the log
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> using(CompletionCallback completionCallback);

        /**
         * During the engine's {@link ReactiveDebeziumEngine#run()} method, call the supplied the supplied function at different
         * stages according to the completion state of each component running within the engine (connectors, tasks etc)
         *
         * @param connectorCallback the callback function; may be null
         * @return this builder object so methods can be chained together; never null
         */
        Builder<R> using(ConnectorCallback connectorCallback);

        /**
         * During the engine's {@link ReactiveDebeziumEngine#run()} method, decide when the offsets
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
        ReactiveDebeziumEngine<R> build();
    }

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link ReactiveDebeziumEngine} instances.
     * The same format is used for key and the value of emitted change events.
     * <p>
     * Convenience method, equivalent to calling {@code create(KeyValueChangeEventFormat.of(MyFormat.class, MyFormat.class)}.
     *
     * @return the new builder; never null
     */
    public static <T> Builder<ChangeEvent<T, T>> create(Class<? extends SerializationFormat<T>> format) {
        return create(format, format);
    }

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link ReactiveDebeziumEngine} instances.
     * Different formats are used for key and the value of emitted change events.
     * <p>
     * Convenience method, equivalent to calling {@code create(KeyValueChangeEventFormat.of(MyKeyFormat.class, MyValueFormat.class)}.
     *
     * @return the new builder; never null
     */
    public static <K, V> Builder<ChangeEvent<K, V>> create(Class<? extends SerializationFormat<K>> keyFormat,
                                                           Class<? extends SerializationFormat<V>> valueFormat) {

        return create(KeyValueChangeEventFormat.of(keyFormat, valueFormat));
    }

    public static <S, T, K extends SerializationFormat<S>, V extends SerializationFormat<T>> Builder<ChangeEvent<S, T>> create(KeyValueChangeEventFormat<K, V> format) {
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
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link ReactiveDebeziumEngine} instances.
     * Emitted change events encapsulate both key and value.
     *
     * @return the new builder; never null
     */
    public static <T, V extends SerializationFormat<T>> Builder<RecordChangeEvent<T>> create(ChangeEventFormat<V> format) {
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
    public static interface BuilderFactory {

        /**
         * Prescribe the output format used by the {@link ReactiveDebeziumEngine}.
         * Usually called by {@link ReactiveDebeziumEngine#create}.
         * @param format
         * @return this builder object so methods can be chained together; never null
         */
        <T, V extends SerializationFormat<T>> Builder<RecordChangeEvent<T>> builder(ChangeEventFormat<V> format);

        /**
         * Prescribe the output format used by the {@link ReactiveDebeziumEngine}.
         * Usually called by {@link ReactiveDebeziumEngine#create}.
         * @param format
         * @return this builder object so methods can be chained together; never null
         */
        <S, T, K extends SerializationFormat<S>, V extends SerializationFormat<T>> Builder<ChangeEvent<S, T>> builder(KeyValueChangeEventFormat<K, V> format);
    }

    Publisher<CommittableRecord<R>> publisher();
}
