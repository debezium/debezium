/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class KafkaTestConfigSource extends TestConfigSource {

    public KafkaTestConfigSource() {
        final Map<String, String> kafkaConfig = new HashMap<>();

        kafkaConfig.put("debezium.sink.type", "kafka");
        kafkaConfig.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        kafkaConfig.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());

        kafkaConfig.put("debezium.source.offset.flush.interval.ms", "0");
        kafkaConfig.put("debezium.source.database.server.name", "testc");
        kafkaConfig.put("debezium.source.schema.include.list", "inventory");
        kafkaConfig.put("debezium.source.table.include.list", "inventory.customers");

        config = kafkaConfig;
    }
}
