/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.jetstream;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class NatsJetStreamTestConfigSource extends TestConfigSource {

    public NatsJetStreamTestConfigSource() {
        Map<String, String> natsJetStreamTest = new HashMap<>();
        natsJetStreamTest.put("debezium.sink.type", "nats-jetstream");
        natsJetStreamTest.put("debezium.sink.nats-jetstream.url",
                NatsJetStreamTestResourceLifecycleManager.getNatsContainerUrl());
        natsJetStreamTest.put("debezium.sink.nats-jetstream.create-stream", "true");
        natsJetStreamTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        natsJetStreamTest.put("debezium.source.topic.prefix", "testc");
        natsJetStreamTest.put("debezium.source.schema.include.list", "inventory");
        natsJetStreamTest.put("debezium.source.table.include.list", "inventory.customers");
        natsJetStreamTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        natsJetStreamTest.put("debezium.source.offset.flush.interval.ms", "0");

        config = natsJetStreamTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
