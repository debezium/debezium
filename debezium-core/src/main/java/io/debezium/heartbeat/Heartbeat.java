/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Field;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * A class that is able to emit heartbeat messages. The clients are
 * supposed to call method {@link #emit(Map, OffsetContext)} from a main loop of a connector.
 *
 * @author Jiri Pechanec
 *
 */
public interface Heartbeat extends AutoCloseable {

    String HEARTBEAT_INTERVAL_PROPERTY_NAME = "heartbeat.interval.ms";

    /**
     * Returns the offset to be used when emitting a heartbeat event. This supplier
     * interface allows for a lazy creation of the offset only when a heartbeat
     * actually is sent, in cases where it's determination is costly.
     */
    @FunctionalInterface
    interface OffsetProducer {
        Map<String, ?> offset();
    }

    Field HEARTBEAT_INTERVAL = Field.create(HEARTBEAT_INTERVAL_PROPERTY_NAME)
            .withDisplayName("Connector heartbeat interval (milli-seconds)")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED_HEARTBEAT, 0))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Length of an interval in milli-seconds in in which the connector periodically sends heartbeat messages "
                    + "to a heartbeat topic. "
                    + "Use 0 to disable heartbeat messages. "
                    + "Disabled by default.")
            .withDefault(DefaultHeartbeat.DEFAULT_HEARTBEAT_INTERVAL)
            .withValidation(Field::isNonNegativeInteger);

    Field HEARTBEAT_TOPICS_PREFIX = Field.create("heartbeat.topics.prefix")
            .withDisplayName("A prefix used for naming of heartbeat topics")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED_HEARTBEAT, 1))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("The prefix that is used to name heartbeat topics."
                    + "Defaults to " + DefaultHeartbeat.DEFAULT_HEARTBEAT_TOPICS_PREFIX + ".")
            .withDefault(DefaultHeartbeat.DEFAULT_HEARTBEAT_TOPICS_PREFIX);

    /**
     * No-op Heartbeat implementation (not enabled)
     */
    Heartbeat DEFAULT_NOOP_HEARTBEAT = () -> false;

    /**
     * Generates a heartbeat record if defined time has elapsed
     * Replaced by {@link ScheduledHeartbeat#emitWithDelay(Map, OffsetContext)}
     *
     * @param partition partition for the heartbeat record
     * @param offset    offset for the heartbeat record
     * @param consumer  - a code to place record among others to be sent into Connect
     */
    @Deprecated
    default void heartbeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        // ignore
    }

    /**
     * Generates a heartbeat record if defined time has elapsed
     * Replaced by {@link ScheduledHeartbeat#emitWithDelay(Map, OffsetContext)}
     *
     * @param partition partition for the heartbeat record
     * @param offsetProducer lazily calculated offset for the heartbeat record
     * @param consumer - a code to place record among others to be sent into Connect
     */
    @Deprecated
    default void heartbeat(Map<String, ?> partition, OffsetProducer offsetProducer, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        // ignore
    }

    /**
     * A class that is able to generate periodic heartbeat messages based on a pre-configured interval. The clients are
     * supposed to call method {@link #emitWithDelay(Map, OffsetContext)} from a main loop of a connector.
     */
    interface ScheduledHeartbeat extends Heartbeat {
        /**
         * Generates a heartbeat record if defined time has elapsed
         *
         * @param partition partition for the heartbeat record
         * @param offset    offset for the heartbeat record
         */
        default void emitWithDelay(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
            // ignore
        }
    }

    /**
     * Generates a heartbeat record unconditionally
     * Replaced by {@link #emit(Map, OffsetContext)}
     *
     * @param partition partition for the heartbeat record
     * @param offset    offset for the heartbeat record
     * @param consumer  - a code to place record among others to be sent into Connect
     */
    @Deprecated
    default void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        // ignore
    }

    /**
     * Generates a heartbeat record unconditionally
     *
     * @param partition partition for the heartbeat record
     * @param offset    offset for the heartbeat record
     */
    default void emit(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
        // ignore
    }

    /**
     * Whether heartbeats are enabled or not.
     */
    boolean isEnabled();

    @Override
    default void close() {
        // default implementations are no-op
    }
}
