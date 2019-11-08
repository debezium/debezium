/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.HeaderConverter;

/**
 * A customized value converter to allow avro message to be delivered as it is (byte[]) to kafka, this is used
 * for outbox pattern where payload is serialized by KafkaAvroSerializer, the consumer need to get the deseralized payload.
 *
 * To enabled the converter in a connector, the following value need to be specified
 * "value.converter": "io.debezium.converters.ByteBufferConverter"
 *
 * @author Yang Yang
 */
public class ByteBufferConverter implements Converter, HeaderConverter {

    private static final ConfigDef CONFIG_DEF = ConverterConfig.newConfigDef();

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        validateSchemaType(schema);
        validateValueType(value);
        return value == null ? null : ((ByteBuffer) value).array();
    }

    private void validateValueType(Object value) {
        if (value != null && !(value instanceof ByteBuffer)) {
            throw new DataException("ByteBufferConverter is not compatible with objects of type " + value.getClass());
        }
    }

    private void validateSchemaType(Schema schema) {
        if (schema != null && schema.type() != Schema.Type.BYTES) {
            throw new DataException("Invalid schema type for ByteBufferConverter: " + schema.type().toString());
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value == null ? null : ByteBuffer.wrap(value));
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return toConnectData(topic, value);
    }

    @Override
    public void close() {
        // do nothing
    }
}
