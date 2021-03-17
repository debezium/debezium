/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Field;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.JdbcConnection;

/**
 * A class that is able to generate periodic heartbeat messages based on a pre-configured interval. The clients are
 * supposed to call method {@link #heartbeat(Map, Map, BlockingConsumer)} from a main loop of a connector.
 *
 * @author Jiri Pechanec
 *
 */
public interface Heartbeat {

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
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Length of an interval in milli-seconds in in which the connector periodically sends heartbeat messages "
                    + "to a heartbeat topic. "
                    + "Use 0 to disable heartbeat messages. "
                    + "Disabled by default.")
            .withDefault(HeartbeatImpl.DEFAULT_HEARTBEAT_INTERVAL)
            .withValidation(Field::isNonNegativeInteger);

    Field HEARTBEAT_TOPICS_PREFIX = Field.create("heartbeat.topics.prefix")
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
    Heartbeat DEFAULT_NOOP_HEARTBEAT = new Heartbeat() {

        @Override
        public void heartbeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        }

        @Override
        public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer)
                throws InterruptedException {
        }

        @Override
        public void heartbeat(Map<String, ?> partition, OffsetProducer offsetProducer,
                              BlockingConsumer<SourceRecord> consumer)
                throws InterruptedException {
        }

        @Override
        public boolean isEnabled() {
            return false;
        }
    };

    /**
     * Generates a heartbeat record if defined time has elapsed
     *
     * @param partition partition for the heartbeat record
     * @param offset offset for the heartbeat record
     * @param consumer - a code to place record among others to be sent into Connect
     */
    // TODO would be nice to pass OffsetContext here; not doing it for now, though, until MySQL is using OffsetContext, too
    void heartbeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException;

    /**
     * Generates a heartbeat record if defined time has elapsed
     *
     * @param partition partition for the heartbeat record
     * @param offsetProducer lazily calculated offset for the heartbeat record
     * @param consumer - a code to place record among others to be sent into Connect
     */
    void heartbeat(Map<String, ?> partition, OffsetProducer offsetProducer, BlockingConsumer<SourceRecord> consumer) throws InterruptedException;

    /**
     * Generates a heartbeat record unconditionaly
     *
     * @param partition partition for the heartbeat record
     * @param offset offset for the heartbeat record
     * @param consumer - a code to place record among others to be sent into Connect
     */
    // TODO would be nice to pass OffsetContext here; not doing it for now, though, until MySQL is using OffsetContext, too
    void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException;

    /**
     * Whether heartbeats are enabled or not.
     */
    boolean isEnabled();

    /**
     * Provide an instance of Heartbeat object
     *
     * @param heartbeatInterval heartbeat interval config value as java.time.Duration
     * @param topicName topic to which the heartbeat messages will be sent
     * @param key kafka partition key to use for the heartbeat message
     */
    static Heartbeat create(Duration heartbeatInterval, String topicName, String key) {
        return heartbeatInterval.isZero() ? DEFAULT_NOOP_HEARTBEAT : new HeartbeatImpl(heartbeatInterval, topicName, key);
    }

    static Heartbeat create(Duration heartbeatInterval, String heartbeatQuery, String topicName, String key, JdbcConnection jdbcConnection,
                            HeartbeatErrorHandler errorHandler) {
        if (heartbeatInterval.isZero()) {
            return DEFAULT_NOOP_HEARTBEAT;
        }

        if (heartbeatQuery != null) {
            return new DatabaseHeartbeatImpl(heartbeatInterval, topicName, key, jdbcConnection, heartbeatQuery, errorHandler);
        }

        return new HeartbeatImpl(heartbeatInterval, topicName, key);
    }
}
