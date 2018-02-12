/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.function.BlockingConsumer;

/**
 * A class that is able to generate periodic heartbeat messages based on a pre-configured interval. The clients are
 * supposed to call method {@link #heartbeat(Consumer)} from a main loop of a connector.
 *
 * @author Jiri Pechanec
 *
 */
public interface Heartbeat {

    public static final Field HEARTBEAT_INTERVAL = Field.create("heartbeat.interval.ms")
            .withDisplayName("Conector heartbeat interval (milli-seconds)")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Length of an interval in milli-seconds in in which the connector periodically sends heartbeat messages "
                    + "to a heartbeat topic. "
                    + "Use 0 to disable heartbeat messages. "
                    + "Disabled by default.")
            .withDefault(HeartbeatImpl.DEFAULT_HEARTBEAT_INTERVAL)
            .withValidation(Field::isNonNegativeInteger);

    public static final Field HEARTBEAT_TOPICS_PREFIX = Field.create("heartbeat.topics.prefix")
           .withDisplayName("A prefix used for naming of heartbeat topics")
           .withType(Type.STRING)
           .withWidth(Width.MEDIUM)
           .withImportance(Importance.LOW)
           .withDescription("The prefix that is used to name heartbeat topics."
                   + "Defaults to " + HeartbeatImpl.DEFAULT_HEARTBEAT_TOPICS_PREFIX + ".")
           .withDefault(HeartbeatImpl.DEFAULT_HEARTBEAT_TOPICS_PREFIX);

    /**
     * No-op Heartbeat implementation
     */
    public static final Heartbeat NULL = new Heartbeat() {
        @Override
        public void heartbeat(BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        }

        @Override
        public void heartbeat(Consumer<SourceRecord> consumer) {
        }
    };

    /**
     * Generates a heartbeat record if defined time has elapsed
     *
     * @param consumer - a code to place record among others to be sent into Connect
     */
    void heartbeat(Consumer<SourceRecord> consumer);

    /**
     * Generates a heartbeat record if defined time has elapsed
     *
     * @param consumer - a code to place record among others to be sent into Connect
     */
    void heartbeat(BlockingConsumer<SourceRecord> consumer) throws InterruptedException;

    /**
     * Provide an instance of Heartbeat object
     *
     * @param configuration - connector configuration
     * @param topicName - topic to which the heartbeat messages will be sent
     * @param positionSupplier - obtain current offset position
     * @return
     */
    public static Heartbeat create(Configuration configuration, String topicName, String key,
            Supplier<OffsetPosition> positionSupplier) {
        return configuration.getDuration(HeartbeatImpl.HEARTBEAT_INTERVAL, ChronoUnit.MILLIS).isZero() ?
            NULL : new HeartbeatImpl(configuration, topicName, key, positionSupplier);
    }
}