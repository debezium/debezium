/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.offset;

import java.util.Collections;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;

/**
 * An auxiliary converter for Kafka {@link io.debezium.storage.kafka.KafkaConnectStorageAdapter.OffsetBackingStore}
 *
 * @author Debezium Authors
 */
public class KafkaOffsetStoreConverter {

    public static Converter jsonConverter() {
        final JsonConverter converter = new JsonConverter();
        converter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);
        return converter;
    }
}
