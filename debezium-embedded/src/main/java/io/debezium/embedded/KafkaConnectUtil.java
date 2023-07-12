/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.SharedTopicAdmin;

/**
 * An auxilliary class that provides internal Kafka Connect related data structures and operations.
 * The code are usually snippets from Kafka Connect runtime codebase.
 *
 * @author Jiri Pechanec
 *
 */
public class KafkaConnectUtil {

    public static final Converter converterForOffsetStore() {
        final JsonConverter converter = new JsonConverter();
        converter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);
        return converter;
    }

    public static final FileOffsetBackingStore fileOffsetBackingStore() {
        return new FileOffsetBackingStore(converterForOffsetStore());
    }

    public static final MemoryOffsetBackingStore memoryOffsetBackingStore() {

        return new MemoryOffsetBackingStore() {

            @Override
            public Set<Map<String, Object>> connectorPartitions(String connectorName) {
                return null;
            }
        };
    }

    public static final KafkaOffsetBackingStore kafkaOffsetBackingStore(Map<String, String> config) {
        final String clientId = "debezium-server";
        final Map<String, Object> adminProps = new HashMap<>(config);
        adminProps.put(CLIENT_ID_CONFIG, clientId + "shared-admin");
        SharedTopicAdmin sharedAdmin = new SharedTopicAdmin(adminProps);

        return new KafkaOffsetBackingStore(null, () -> clientId, converterForOffsetStore());
    }
}
