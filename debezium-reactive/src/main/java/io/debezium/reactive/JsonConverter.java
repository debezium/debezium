/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

public class JsonConverter implements Converter<String> {

    final org.apache.kafka.connect.json.JsonConverter delegate = new org.apache.kafka.connect.json.JsonConverter();

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     */
    public void configure(Map<String, ?> configs) {
        delegate.configure(configs, false);
    }

    /**
     * Convert a Kafka Connect @{link SourceRecord} object to a JSON string representation.
     * @param record the record created by plugin
     * @return the converted value
     */
    public String fromConnectData(SourceRecord record) {
        return new String(delegate.fromConnectData("dummy", record.valueSchema(), record.value()));
    }

}
