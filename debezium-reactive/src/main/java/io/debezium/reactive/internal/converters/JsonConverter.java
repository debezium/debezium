/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal.converters;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.reactive.converters.AsJson;
import io.debezium.reactive.spi.AsType;
import io.debezium.reactive.spi.Converter;

/**
 * A converter of {@link SourceRecord} to a JSON string.
 *
 * @author Jiri Pechanec
 *
 */
public class JsonConverter implements Converter<String> {

    private static final String DUMMY_TOPIC = "dummy";
    final org.apache.kafka.connect.json.JsonConverter keyDelegate = new org.apache.kafka.connect.json.JsonConverter();
    final org.apache.kafka.connect.json.JsonConverter valueDelegate = new org.apache.kafka.connect.json.JsonConverter();

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     */
    public void configure(Map<String, ?> configs) {
        keyDelegate.configure(configs, true);
        valueDelegate.configure(configs, false);
    }

    /**
     * Convert a key from Kafka Connect @{link SourceRecord} object to a JSON string representation.
     * @param record the record created by plugin
     * @return the converted key
     */
    @Override
    public String convertKey(SourceRecord record) {
        return new String(keyDelegate.fromConnectData(DUMMY_TOPIC, record.keySchema(), record.key()));
    }

    /**
     * Convert a value from Kafka Connect @{link SourceRecord} object to a JSON string representation.
     * @param record the record created by plugin
     * @return the converted value
     */
    @Override
    public String convertValue(SourceRecord record) {
        return new String(valueDelegate.fromConnectData(DUMMY_TOPIC, record.valueSchema(), record.value()));
    }

    @Override
    public Class<? extends AsType<String>> getConvertedType() {
        return AsJson.class;
    }
}
