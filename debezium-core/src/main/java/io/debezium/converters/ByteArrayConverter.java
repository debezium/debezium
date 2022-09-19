/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters;

import java.io.IOException;
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
 * "value.converter": "io.debezium.converters.ByteArrayConverter"
 *
 * @author Nathan Bradshaw
 */
public class ByteArrayConverter implements Converter, HeaderConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayConverter.class);
    private static final ConfigDef CONFIG_DEF;

    protected static final String DELEGATE_CONVERTER_TYPE = "delegate.converter.type";

    private Converter delegateConverter;

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
            delegateConverter = Instantiator.getInstance(converterTypeName);
            delegateConverter.configure(Configuration.from(configs).subset(DELEGATE_CONVERTER_TYPE, true).asMap(), isKey);
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema != null && schema.type() != Schema.Type.BYTES) {
            assertDelegateProvided(topic, value);
            LOGGER.debug("Value is not of Schema.Type.BYTES, delegating to " + delegateConverter.getClass().getName());
            return delegateConverter.fromConnectData(topic, schema, value);
        }
        else if (value != null && !(value instanceof byte[])) {
            assertDelegateProvided(topic, value);
            LOGGER.debug("Value is not of Schema.Type.BYTES, delegating to " + delegateConverter.getClass().getName());
            return delegateConverter.fromConnectData(topic, schema, value);
        }
        else {
            return (byte[]) value;
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value);
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return this.fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return this.toConnectData(topic, value);
    }

    @Override
    public void close() throws IOException {

    }

    private void assertDelegateProvided(String name, Object type) {
        if (delegateConverter == null) {
            throw new DataException("A " + name + " of type '" + type + "' requires a delegate.converter.type to be configured");
        }
    }
}
