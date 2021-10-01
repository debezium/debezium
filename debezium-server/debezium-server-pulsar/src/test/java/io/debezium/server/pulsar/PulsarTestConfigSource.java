/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pulsar;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class PulsarTestConfigSource extends TestConfigSource {

    public PulsarTestConfigSource() {
        Map<String, String> pulsarTest = new HashMap<>();

        pulsarTest.put("debezium.sink.type", "pulsar");
        pulsarTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        pulsarTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        pulsarTest.put("debezium.source.offset.flush.interval.ms", "0");
        pulsarTest.put("debezium.source.database.server.name", "testc");
        pulsarTest.put("debezium.source.schema.include.list", "inventory");
        pulsarTest.put("debezium.source.table.include.list", "inventory.customers,inventory.nokey");

        config = pulsarTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
