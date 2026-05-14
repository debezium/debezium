/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.chroniclequeue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;

import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

/**
 * Serializes and deserializes {@link SourceRecord} instances to and from Chronicle Queue
 * wire format using Kafka Connect's {@link JsonConverter} for key/value data and Jackson's
 * {@link ObjectMapper} for source partition and offset maps.
 *
 * @author Chris Cranford
 */
class SourceRecordJsonSerializer {

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() {
    };

    private final JsonConverter keyConverter;
    private final JsonConverter valueConverter;
    private final ObjectMapper objectMapper;

    SourceRecordJsonSerializer() {
        this.keyConverter = new JsonConverter();
        Map<String, String> keyConfig = new HashMap<>();
        keyConfig.put("schemas.enable", "true");
        keyConverter.configure(keyConfig, true);

        this.valueConverter = new JsonConverter();
        Map<String, String> valueConfig = new HashMap<>();
        valueConfig.put("schemas.enable", "true");
        valueConverter.configure(valueConfig, false);

        this.objectMapper = new ObjectMapper();
    }

    void write(SourceRecord record, WireOut wire) {
        String topic = record.topic();
        wire.write("topic").text(topic);

        Integer kafkaPartition = record.kafkaPartition();
        wire.write("kafkaPartitionPresent").bool(kafkaPartition != null);
        wire.write("kafkaPartition").int32(kafkaPartition != null ? kafkaPartition : -1);

        Long timestamp = record.timestamp();
        wire.write("timestampPresent").bool(timestamp != null);
        wire.write("timestamp").int64(timestamp != null ? timestamp : 0L);

        try {
            byte[] partitionBytes = objectMapper.writeValueAsBytes(record.sourcePartition());
            wire.write("sourcePartition").bytes(partitionBytes);
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to serialize sourcePartition", e);
        }

        try {
            byte[] offsetBytes = objectMapper.writeValueAsBytes(record.sourceOffset());
            wire.write("sourceOffset").bytes(offsetBytes);
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to serialize sourceOffset", e);
        }

        byte[] keyBytes = keyConverter.fromConnectData(topic, record.keySchema(), record.key());
        wire.write("keyPresent").bool(keyBytes != null);
        if (keyBytes != null) {
            wire.write("key").bytes(keyBytes);
        }

        byte[] valueBytes = valueConverter.fromConnectData(topic, record.valueSchema(), record.value());
        wire.write("valuePresent").bool(valueBytes != null);
        if (valueBytes != null) {
            wire.write("value").bytes(valueBytes);
        }

        int headerCount = 0;
        if (record.headers() != null) {
            for (Header ignored : record.headers()) {
                headerCount++;
            }
        }
        wire.write("headerCount").int32(headerCount);

        if (headerCount > 0) {
            int i = 0;
            for (Header header : record.headers()) {
                wire.write("h" + i + "k").text(header.key());
                byte[] headerBytes = valueConverter.fromConnectData(topic, header.schema(), header.value());
                wire.write("h" + i + "p").bool(headerBytes != null);
                if (headerBytes != null) {
                    wire.write("h" + i + "v").bytes(headerBytes);
                }
                i++;
            }
        }
    }

    SourceRecord read(WireIn wire) {
        String topic = wire.read("topic").text();

        boolean kafkaPartitionPresent = wire.read("kafkaPartitionPresent").bool();
        int kafkaPartitionRaw = wire.read("kafkaPartition").int32();
        Integer kafkaPartition = kafkaPartitionPresent ? kafkaPartitionRaw : null;

        boolean timestampPresent = wire.read("timestampPresent").bool();
        long timestampRaw = wire.read("timestamp").int64();
        Long timestamp = timestampPresent ? timestampRaw : null;

        Map<String, Object> sourcePartition;
        try {
            byte[] partitionBytes = wire.read("sourcePartition").bytes();
            sourcePartition = partitionBytes != null
                    ? objectMapper.readValue(partitionBytes, MAP_TYPE_REF)
                    : null;
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to deserialize sourcePartition", e);
        }

        Map<String, Object> sourceOffset;
        try {
            byte[] offsetBytes = wire.read("sourceOffset").bytes();
            sourceOffset = offsetBytes != null
                    ? objectMapper.readValue(offsetBytes, MAP_TYPE_REF)
                    : null;
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to deserialize sourceOffset", e);
        }

        boolean keyPresent = wire.read("keyPresent").bool();
        Schema keySchema = null;
        Object key = null;
        if (keyPresent) {
            byte[] keyBytes = wire.read("key").bytes();
            SchemaAndValue schemaAndValue = keyConverter.toConnectData(topic, keyBytes);
            keySchema = schemaAndValue.schema();
            key = schemaAndValue.value();
        }

        boolean valuePresent = wire.read("valuePresent").bool();
        Schema valueSchema = null;
        Object value = null;
        if (valuePresent) {
            byte[] valueBytes = wire.read("value").bytes();
            SchemaAndValue schemaAndValue = valueConverter.toConnectData(topic, valueBytes);
            valueSchema = schemaAndValue.schema();
            value = schemaAndValue.value();
        }

        int headerCount = wire.read("headerCount").int32();
        ConnectHeaders headers = new ConnectHeaders();
        for (int i = 0; i < headerCount; i++) {
            String headerKey = wire.read("h" + i + "k").text();
            boolean headerPresent = wire.read("h" + i + "p").bool();
            if (headerPresent) {
                byte[] headerBytes = wire.read("h" + i + "v").bytes();
                SchemaAndValue schemaAndValue = valueConverter.toConnectData(topic, headerBytes);
                headers.add(headerKey, schemaAndValue);
            }
            else {
                headers.add(headerKey, new SchemaAndValue(null, null));
            }
        }

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                kafkaPartition,
                keySchema,
                key,
                valueSchema,
                value,
                timestamp,
                headers);
    }
}