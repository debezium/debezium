/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bindings.kafka;

import org.apache.kafka.connect.data.Struct;

/**
 * A Kafka-specific representation of a {@link io.debezium.sink.DebeziumSinkRecord} that offers access to Kafka message headers and coordinates (offset, partition, offset).
 * @author rk3rn3r
 */
public interface DebeziumSinkRecord extends io.debezium.sink.DebeziumSinkRecord {

    /**
     * Returns a (Kafka) Struct with the Kafka message coordinates (offset, partition, offset).
     * @return Kafka message coordinates
     */
    Struct kafkaCoordinates();

    /**
     * Returns a (Kafka) Struct with the Kafka message headers.
     * @return Kafka message headers
     */
    Struct kafkaHeader();

}
