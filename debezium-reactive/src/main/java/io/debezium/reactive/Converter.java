/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

public interface Converter<T> {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     */
    void configure(Map<String, ?> configs);

    /**
     * Convert a Kafka Connect @{link SourceRecord} object to another object for downstream processing.
     * @param record the record created by plugin
     * @return the converted value
     */
    T fromConnectData(SourceRecord record);

}
