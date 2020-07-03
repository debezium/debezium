package io.debezium.embedded;

import io.debezium.engine.format.SerializationFormat;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

public class EmbeddedEngineChangeEventFactory {
    private static final String TOPIC_NAME = "debezium";

    public static <K, V> EmbeddedEngineChangeEvent<K, V> from(Class<? extends SerializationFormat<?>> serializationFormat,
                                                              Converter keyConverter,
                                                              Converter valueConverter,
                                                              SourceRecord sourceRecord) {
        if (serializationFormat == Connect.class) {
            return (EmbeddedEngineChangeEvent<K, V>) new EmbeddedEngineChangeEvent<Void, SourceRecord>(
                    null,
                    sourceRecord,
                    sourceRecord);
        } else {
            final byte[] key = keyConverter.fromConnectData(TOPIC_NAME, sourceRecord.keySchema(), sourceRecord.key());
            final byte[] value = valueConverter.fromConnectData(TOPIC_NAME, sourceRecord.valueSchema(), sourceRecord.value());
            return (EmbeddedEngineChangeEvent<K, V>) new EmbeddedEngineChangeEvent<>(
                    key != null ? new String(key) : null,
                    value != null ? new String(value) : null,
                    sourceRecord);
        }
    }
}
