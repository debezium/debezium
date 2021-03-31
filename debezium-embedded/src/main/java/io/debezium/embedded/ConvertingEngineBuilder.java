/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.IOException;
import java.time.Clock;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.Builder;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.CloudEvents;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.Protobuf;
import io.debezium.engine.format.SerializationFormat;
import io.debezium.engine.spi.OffsetCommitPolicy;

/**
 * A builder that creates a decorator around {@link EmbeddedEngine} that is responsible for the conversion
 * to the final format.
 *
 * @author Jiri Pechanec
 */
public class ConvertingEngineBuilder<R> implements Builder<R> {

    private static final String CONVERTER_PREFIX = "converter";
    private static final String KEY_CONVERTER_PREFIX = "key.converter";
    private static final String VALUE_CONVERTER_PREFIX = "value.converter";
    private static final String FIELD_CLASS = "class";
    private static final String TOPIC_NAME = "debezium";
    private static final String APICURIO_SCHEMA_REGISTRY_URL_CONFIG = "apicurio.registry.url";

    private final Builder<SourceRecord> delegate;
    private final Class<? extends SerializationFormat<?>> formatKey;
    private final Class<? extends SerializationFormat<?>> formatValue;
    private Configuration config;

    private Function<SourceRecord, R> toFormat;
    private Function<R, SourceRecord> fromFormat;

    ConvertingEngineBuilder(ChangeEventFormat<?> format) {
        this.delegate = EmbeddedEngine.create();
        this.formatKey = null;
        this.formatValue = format.getValueFormat();
    }

    ConvertingEngineBuilder(KeyValueChangeEventFormat<?, ?> format) {
        this.delegate = EmbeddedEngine.create();
        this.formatKey = format.getKeyFormat();
        this.formatValue = format.getValueFormat();
    }

    @Override
    public Builder<R> notifying(Consumer<R> consumer) {
        delegate.notifying((record) -> consumer.accept(toFormat.apply(record)));
        return this;
    }

    private boolean isFormat(Class<? extends SerializationFormat<?>> format1, Class<? extends SerializationFormat<?>> format2) {
        return format1 == (Class<?>) format2;
    }

    @Override
    public Builder<R> notifying(ChangeConsumer<R> handler) {
        delegate.notifying(
                (records, committer) -> handler.handleBatch(records.stream()
                        .map(x -> toFormat.apply(x))
                        .collect(Collectors.toList()),
                        new RecordCommitter<R>() {

                            @Override
                            public void markProcessed(R record) throws InterruptedException {
                                committer.markProcessed(fromFormat.apply(record));
                            }

                            @Override
                            public void markBatchFinished() throws InterruptedException {
                                committer.markBatchFinished();
                            }

                            @Override
                            public void markProcessed(R record, DebeziumEngine.Offsets sourceOffsets) throws InterruptedException {
                                committer.markProcessed(fromFormat.apply(record), sourceOffsets);
                            }

                            @Override
                            public DebeziumEngine.Offsets buildOffsets() {
                                return committer.buildOffsets();
                            }
                        }));
        return this;
    }

    @Override
    public Builder<R> using(Properties config) {
        this.config = Configuration.from(config);
        delegate.using(config);
        return this;
    }

    @Override
    public Builder<R> using(ClassLoader classLoader) {
        delegate.using(classLoader);
        return this;
    }

    @Override
    public Builder<R> using(Clock clock) {
        delegate.using(clock);
        return this;
    }

    @Override
    public Builder<R> using(CompletionCallback completionCallback) {
        delegate.using(completionCallback);
        return this;
    }

    @Override
    public Builder<R> using(ConnectorCallback connectorCallback) {
        delegate.using(connectorCallback);
        return this;
    }

    @Override
    public Builder<R> using(OffsetCommitPolicy policy) {
        delegate.using(policy);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumEngine<R> build() {
        final DebeziumEngine<SourceRecord> engine = delegate.build();
        Converter keyConverter;
        Converter valueConverter;

        if (formatValue == Connect.class) {
            toFormat = (record) -> {
                return (R) new EmbeddedEngineChangeEvent<Void, SourceRecord>(
                        null,
                        record,
                        record);
            };
        }
        else {
            keyConverter = createConverter(formatKey, true);
            valueConverter = createConverter(formatValue, false);
            toFormat = (record) -> {
                final byte[] key = keyConverter.fromConnectData(TOPIC_NAME, record.keySchema(), record.key());
                final byte[] value = valueConverter.fromConnectData(TOPIC_NAME, record.valueSchema(), record.value());
                return isFormat(formatKey, Json.class) && isFormat(formatValue, Json.class)
                        || isFormat(formatValue, CloudEvents.class)
                                ? (R) new EmbeddedEngineChangeEvent<String, String>(
                                        key != null ? new String(key) : null,
                                        value != null ? new String(value) : null,
                                        record)
                                : (R) new EmbeddedEngineChangeEvent<byte[], byte[]>(
                                        key,
                                        value,
                                        record);
            };
        }

        fromFormat = (record) -> ((EmbeddedEngineChangeEvent<?, ?>) record).sourceRecord();

        return new DebeziumEngine<R>() {

            @Override
            public void run() {
                engine.run();
            }

            @Override
            public void close() throws IOException {
                engine.close();
            }
        };
    }

    private Converter createConverter(Class<? extends SerializationFormat<?>> format, boolean key) {
        // The converters can be configured both using converter.* prefix for cases when both converters
        // are the same or using key.converter.* and value.converter.* converter when converters
        // are different for key and value
        Configuration converterConfig = config.subset(key ? KEY_CONVERTER_PREFIX : VALUE_CONVERTER_PREFIX, true);
        final Configuration commonConverterConfig = config.subset(CONVERTER_PREFIX, true);
        converterConfig = commonConverterConfig.edit().with(converterConfig).build();

        if (isFormat(format, Json.class)) {
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
        }
        else if (isFormat(format, Protobuf.class)) {
            converterConfig = converterConfig.edit().withDefault(FIELD_CLASS, "io.confluent.connect.protobuf.ProtobufConverter").build();
        }
        else {
            throw new DebeziumException("Converter '" + format.getSimpleName() + "' is not supported");
        }
        final Converter converter = converterConfig.getInstance(FIELD_CLASS, Converter.class);
        converter.configure(converterConfig.asMap(), key);
        return converter;
    }
}
