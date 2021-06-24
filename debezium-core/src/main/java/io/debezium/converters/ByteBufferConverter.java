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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Instantiator;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferConverter.class);

    public static final String DELEGATE_CONVERTER_TYPE = "delegate.converter.type";

    private Converter delegateConverter;
    private static final ConfigDef CONFIG_DEF;

    static {
        CONFIG_DEF = ConverterConfig.newConfigDef();
        CONFIG_DEF.define(DELEGATE_CONVERTER_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Specifies the delegate converter class");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        final String converterTypeName = (String) configs.get(DELEGATE_CONVERTER_TYPE);
        if (converterTypeName != null) {
            delegateConverter = Instantiator.getInstance(converterTypeName, () -> getClass().getClassLoader(), null);
            delegateConverter.configure(Configuration.from(configs).subset(DELEGATE_CONVERTER_TYPE, true).asMap(), isKey);
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema != null && schema.type() != Schema.Type.BYTES) {
            assertDataException("schema", schema.type());
            LOGGER.debug("Value is not of Schema.Type.BYTES, delegating to " + delegateConverter.getClass().getName());
            return delegateConverter.fromConnectData(topic, schema, value);
        }
        else if (value != null && !(value instanceof ByteBuffer)) {
            assertDataException("value", value.getClass().getName());
            LOGGER.debug("Value is not of type ByteBuffer, delegating to " + delegateConverter.getClass().getName());
            return delegateConverter.fromConnectData(topic, schema, value);
        }
        return value == null ? null : ((ByteBuffer) value).array();
    }

    private void assertDataException(String name, Object type) {
        if (delegateConverter == null) {
            throw new DataException("A " + name + " of type '" + type + "' requires a delegate.converter.type to be configured");
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
