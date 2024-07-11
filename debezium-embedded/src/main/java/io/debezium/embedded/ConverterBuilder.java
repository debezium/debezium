/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.HeaderConverter;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.Binary;
import io.debezium.engine.format.CloudEvents;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.JsonByteArray;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.Protobuf;
import io.debezium.engine.format.SerializationFormat;
import io.debezium.engine.format.SimpleString;

/**
 * A builder which creates converter functions for requested format.
 */
public class ConverterBuilder<R> {

    private static final String CONVERTER_PREFIX = "converter";
    private static final String HEADER_CONVERTER_PREFIX = "header.converter";
    private static final String KEY_CONVERTER_PREFIX = "key.converter";
    private static final String VALUE_CONVERTER_PREFIX = "value.converter";
    private static final String FIELD_CLASS = "class";
    private static final String TOPIC_NAME = "debezium";
    private static final String APICURIO_SCHEMA_REGISTRY_URL_CONFIG = "apicurio.registry.url";

    private Class<? extends SerializationFormat<?>> formatHeader;
    private Class<? extends SerializationFormat<?>> formatKey;
    private Class<? extends SerializationFormat<?>> formatValue;
    private Configuration config;

    public ConverterBuilder<R> using(KeyValueHeaderChangeEventFormat<?, ?, ?> format) {
        this.formatKey = format.getKeyFormat();
        this.formatValue = format.getValueFormat();
        this.formatHeader = format.getHeaderFormat();
        return this;
    }

    public ConverterBuilder<R> using(Properties config) {
        this.config = Configuration.from(config);
        return this;
    }

    public HeaderConverter headerConverter() {
        if (formatValue == Connect.class) {
            return null;
        }
        else {
            if (formatHeader == null) {
                return null;
            }
            else {
                return createHeaderConverter(formatHeader);
            }
        }
    }

    public Function<SourceRecord, R> toFormat(HeaderConverter headerConverter) {
        Function<SourceRecord, R> toFormat;

        Converter keyConverter;
        Converter valueConverter;

        if (formatValue == Connect.class) {
            toFormat = (record) -> (R) new EmbeddedEngineChangeEvent<Void, SourceRecord, Object>(
                    null,
                    record,
                    StreamSupport.stream(record.headers().spliterator(), false)
                            .map(EmbeddedEngineHeader::new).collect(Collectors.toList()),
                    record);
        }
        else {
            keyConverter = createConverter(formatKey, true);
            valueConverter = createConverter(formatValue, false);

            toFormat = (record) -> {
                String topicName = record.topic();
                if (topicName == null) {
                    topicName = TOPIC_NAME;
                }
                final byte[] key = keyConverter.fromConnectData(topicName, record.keySchema(), record.key());
                final byte[] value = valueConverter.fromConnectData(topicName, record.valueSchema(), record.value());

                List<Header<?>> headers = Collections.emptyList();
                if (headerConverter != null) {
                    List<Header<byte[]>> byteArrayHeaders = convertHeaders(record, topicName, headerConverter);
                    headers = (List) byteArrayHeaders;
                    if (shouldConvertHeadersToString()) {
                        headers = byteArrayHeaders.stream()
                                .map(h -> new EmbeddedEngineHeader<>(h.getKey(), new String(h.getValue(), StandardCharsets.UTF_8)))
                                .collect(Collectors.toList());
                    }
                }
                Object convertedKey = key;
                Object convertedValue = value;
                if (key != null && shouldConvertKeyToString()) {
                    convertedKey = new String(key, StandardCharsets.UTF_8);
                }
                if (value != null && shouldConvertValueToString()) {
                    convertedValue = new String(value, StandardCharsets.UTF_8);
                }
                return (R) new EmbeddedEngineChangeEvent<>(convertedKey, convertedValue, (List) headers, record);
            };
        }

        return toFormat;
    }

    public Function<R, SourceRecord> fromFormat() {
        return (record) -> ((EmbeddedEngineChangeEvent<?, ?, ?>) record).sourceRecord();
    }

    private static boolean isFormat(Class<? extends SerializationFormat<?>> format1, Class<? extends SerializationFormat<?>> format2) {
        return format1 == format2;
    }

    private boolean shouldConvertKeyToString() {
        return isFormat(formatKey, Json.class) || isFormat(formatKey, SimpleString.class);
    }

    private boolean shouldConvertValueToString() {
        return isFormat(formatValue, Json.class) || isFormat(formatValue, SimpleString.class) || isFormat(formatValue, CloudEvents.class);
    }

    private boolean shouldConvertHeadersToString() {
        return isFormat(formatHeader, Json.class);
    }

    private List<Header<byte[]>> convertHeaders(
                                                SourceRecord record, String topicName, HeaderConverter headerConverter) {
        List<Header<byte[]>> headers = new ArrayList<>();

        for (org.apache.kafka.connect.header.Header header : record.headers()) {
            String headerKey = header.key();
            byte[] rawHeader = headerConverter.fromConnectHeader(topicName, headerKey, header.schema(), header.value());
            headers.add(new EmbeddedEngineHeader<>(headerKey, rawHeader));
        }

        return headers;
    }

    private HeaderConverter createHeaderConverter(Class<? extends SerializationFormat<?>> format) {
        Configuration converterConfig = config.subset(HEADER_CONVERTER_PREFIX, true);
        final Configuration commonConverterConfig = config.subset(CONVERTER_PREFIX, true);
        converterConfig = commonConverterConfig.edit().with(converterConfig)
                .with(ConverterConfig.TYPE_CONFIG, "header")
                .build();

        if (isFormat(format, Json.class) || isFormat(format, JsonByteArray.class)) {
            converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "org.apache.kafka.connect.json.JsonConverter").build();
        }
        else if (isFormat(format, ClientProvided.class)) {
            if (converterConfig.getString(FIELD_CLASS) == null) {
                throw new DebeziumException(
                        "`" + ClientProvided.class.getSimpleName().toLowerCase() + "`" + " header converter requires a '" + FIELD_CLASS + "' configuration");
            }
        }
        else {
            throw new DebeziumException("Header Converter '" + format.getSimpleName() + "' is not supported");
        }
        final HeaderConverter converter = converterConfig.getInstance(FIELD_CLASS, HeaderConverter.class);
        converter.configure(converterConfig.asMap());
        return converter;
    }

    private Converter createConverter(Class<? extends SerializationFormat<?>> format, boolean key) {
        // The converters can be configured both using converter.* prefix for cases when both converters
        // are the same or using key.converter.* and value.converter.* converter when converters
        // are different for key and value
        Configuration converterConfig = config.subset(key ? KEY_CONVERTER_PREFIX : VALUE_CONVERTER_PREFIX, true);
        final Configuration commonConverterConfig = config.subset(CONVERTER_PREFIX, true);
        converterConfig = commonConverterConfig.edit().with(converterConfig).build();

        if (isFormat(format, Json.class) || isFormat(format, JsonByteArray.class)) {
            if (converterConfig.hasKey(APICURIO_SCHEMA_REGISTRY_URL_CONFIG)) {
                converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "io.apicurio.registry.utils.converter.ExtJsonConverter").build();
            }
            else {
                converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "org.apache.kafka.connect.json.JsonConverter").build();
            }
        }
        else if (isFormat(format, CloudEvents.class)) {
            converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "io.debezium.converters.CloudEventsConverter").build();
        }
        else if (isFormat(format, Avro.class)) {
            if (converterConfig.hasKey(APICURIO_SCHEMA_REGISTRY_URL_CONFIG)) {
                converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "io.apicurio.registry.utils.converter.AvroConverter").build();
            }
            else {
                converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "io.confluent.connect.avro.AvroConverter").build();
            }
            converterConfig = converterConfig.edit()
                    .withDefault(CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, CommonConnectorConfig.SchemaNameAdjustmentMode.AVRO)
                    .build();
        }
        else if (isFormat(format, Protobuf.class)) {
            converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "io.confluent.connect.protobuf.ProtobufConverter").build();
        }
        else if (isFormat(format, Binary.class)) {
            converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "io.debezium.converters.BinaryDataConverter").build();
        }
        else if (isFormat(format, SimpleString.class)) {
            converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "org.apache.kafka.connect.storage.StringConverter").build();
        }
        else if (isFormat(format, ClientProvided.class)) {
            if (converterConfig.getString(FIELD_CLASS) == null) {
                throw new DebeziumException(
                        "`" + ClientProvided.class.getSimpleName().toLowerCase() + "`" + (key ? " key" : " value") + " converter requires a '" + FIELD_CLASS
                                + "' configuration");
            }
        }
        else {
            throw new DebeziumException("Converter '" + format.getSimpleName() + "' is not supported");
        }
        final Converter converter = converterConfig.getInstance(FIELD_CLASS, Converter.class);
        converter.configure(converterConfig.asMap(), key);
        return converter;
    }
}
