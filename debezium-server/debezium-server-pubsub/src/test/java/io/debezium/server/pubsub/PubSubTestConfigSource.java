/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class PubSubTestConfigSource extends TestConfigSource {

    public PubSubTestConfigSource() {
        Map<String, String> pubsubTest = new HashMap<>();

        pubsubTest.put("debezium.sink.type", "pubsub");
        pubsubTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        pubsubTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        pubsubTest.put("debezium.source.offset.flush.interval.ms", "0");
        pubsubTest.put("debezium.source.database.server.name", "testc");
        pubsubTest.put("debezium.source.schema.include.list", "inventory");
        pubsubTest.put("debezium.source.table.include.list", "inventory.customers");

        config = pubsubTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}