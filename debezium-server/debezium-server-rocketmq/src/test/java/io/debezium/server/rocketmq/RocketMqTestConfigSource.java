/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rocketmq;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class RocketMqTestConfigSource extends TestConfigSource {

    public RocketMqTestConfigSource() {

        final Map<String, String> rocketmqConfig = new HashMap<>();

        rocketmqConfig.put("debezium.sink.type", "rocketmq");
        rocketmqConfig.put("debezium.sink.rocketmq.producer.name.srv.addr", RocketMqTestResourceLifecycleManager.getNamesrvAddr());
        rocketmqConfig.put("debezium.sink.rocketmq.producer.group", RocketMqTestResourceLifecycleManager.getGroup());
        rocketmqConfig.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        rocketmqConfig.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        rocketmqConfig.put("debezium.source.offset.flush.interval.ms", "0");
        rocketmqConfig.put("debezium.source.topic.prefix", "testc");
        rocketmqConfig.put("debezium.source.schema.include.list", "inventory");
        rocketmqConfig.put("debezium.source.table.include.list", "inventory.customers");
        config = rocketmqConfig;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
