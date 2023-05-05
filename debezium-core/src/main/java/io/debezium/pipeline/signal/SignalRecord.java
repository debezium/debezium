/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;

/**
 * The class represent the signal sent on a channel:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 *
 * @author Mario Fiore Vitale
 */
public class SignalRecord {
    private final String id;
    private final String type;
    private final String data;
    private final Map<String, Object> additionalData;

    public SignalRecord(String id, String type, String data, Long channelOffset, Map<String, Object> additionalData) {
        this.id = id;
        this.type = type;
        this.data = data;
        this.additionalData = additionalData;
    }

    public static Optional<SignalRecord> buildSignalRecordFromChangeEventSource(Struct value, CommonConnectorConfig config) {

        final Optional<String[]> parseSignal = config.parseSignallingMessage(value);

        return parseSignal.map(signalMessage -> new SignalRecord(signalMessage[0], signalMessage[1], signalMessage[2], null, Map.of()));
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getData() {
        return data;
    }

    public <T> T getAdditionalDataProperty(String property, Class<T> type) {
        return type.cast(additionalData.get(property));
    }

    public Map<String, Object> getAdditionalData() {
        return additionalData;
    }

    @Override
    public String toString() {
        return "SignalRecord{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", data='" + data + '\'' +
                ", additionalData=" + additionalData +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SignalRecord that = (SignalRecord) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
