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
        kafkaConfig.put("debezium.sink.kafka.producer.bootstrap.servers", KafkaTestResourceLifecycleManager.getBootstrapServers());
        kafkaConfig.put("debezium.sink.kafka.producer.key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.put("debezium.sink.kafka.producer.value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaConfig.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        kafkaConfig.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());

        kafkaConfig.put("debezium.source.offset.flush.interval.ms", "0");
        kafkaConfig.put("debezium.source.topic.prefix", "testc");
        kafkaConfig.put("debezium.source.schema.include.list", "inventory");
        kafkaConfig.put("debezium.source.table.include.list", "inventory.customers");
        kafkaConfig.put("debezium.format.header.schemas.enable", "false");
        // DBZ-5105
        kafkaConfig.put("debezium.sink.kafka.producer.ssl.endpoint.identification.algorithm", "");

        kafkaConfig.put("debezium.transforms", "addheader");
        kafkaConfig.put("debezium.transforms.addheader.type", "io.debezium.server.kafka.AddHeaderTransform");

        config = kafkaConfig;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
