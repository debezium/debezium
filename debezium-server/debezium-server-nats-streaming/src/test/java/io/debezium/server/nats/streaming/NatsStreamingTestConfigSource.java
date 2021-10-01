/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.streaming;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class NatsStreamingTestConfigSource extends TestConfigSource {

    public NatsStreamingTestConfigSource() {
        Map<String, String> natsStreamingTest = new HashMap<>();

        natsStreamingTest.put("debezium.sink.type", "nats-streaming");
        natsStreamingTest.put("debezium.sink.nats-streaming.url",
                NatsStreamingTestResourceLifecycleManager.getNatsStreamingContainerUrl());
        natsStreamingTest.put("debezium.sink.nats-streaming.cluster.id", "debezium");
        natsStreamingTest.put("debezium.sink.nats-streaming.client.id", "debezium-sink");
        natsStreamingTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        natsStreamingTest.put("debezium.source.database.server.name", "testc");
        natsStreamingTest.put("debezium.source.schema.include.list", "inventory");
        natsStreamingTest.put("debezium.source.table.include.list", "inventory.customers");
        natsStreamingTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        natsStreamingTest.put("debezium.source.offset.flush.interval.ms", "0");

        config = natsStreamingTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
