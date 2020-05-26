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
import io.debezium.server.TestDatabase;

public class PubSubTestConfigSource extends TestConfigSource {

    final Map<String, String> pubsubTest = new HashMap<>();

    public PubSubTestConfigSource() {
        pubsubTest.put("debezium.sink.type", "pubsub");
        pubsubTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        pubsubTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        pubsubTest.put("debezium.source.offset.flush.interval.ms", "0");
        pubsubTest.put("debezium.source.database.hostname", TestDatabase.POSTGRES_HOST);
        pubsubTest.put("debezium.source.database.port", Integer.toString(TestDatabase.POSTGRES_PORT));
        pubsubTest.put("debezium.source.database.user", TestDatabase.POSTGRES_USER);
        pubsubTest.put("debezium.source.database.password", TestDatabase.POSTGRES_PASSWORD);
        pubsubTest.put("debezium.source.database.dbname", TestDatabase.POSTGRES_DBNAME);
        pubsubTest.put("debezium.source.database.server.name", "testc");
        pubsubTest.put("debezium.source.schema.whitelist", "inventory");
        pubsubTest.put("debezium.source.table.whitelist", "inventory.customers");

        config = pubsubTest;
    }
}