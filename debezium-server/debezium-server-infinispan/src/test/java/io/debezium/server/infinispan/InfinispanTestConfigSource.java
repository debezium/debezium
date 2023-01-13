/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.infinispan;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class InfinispanTestConfigSource extends TestConfigSource {

    public static final String CACHE_NAME = "debezium_test";
    public static final String USER_NAME = "admin";
    public static final String PASSWORD = "secret";
    public static final String CONFIG_FILE = "infinispan-local.xml";

    public InfinispanTestConfigSource() {
        Map<String, String> infinispanTest = new HashMap<>();

        infinispanTest.put("debezium.sink.type", "infinispan");
        infinispanTest.put("debezium.sink.infinispan.cache", CACHE_NAME);
        infinispanTest.put("debezium.sink.infinispan.user", USER_NAME);
        infinispanTest.put("debezium.sink.infinispan.password", PASSWORD);
        infinispanTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        infinispanTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        infinispanTest.put("debezium.source.offset.flush.interval.ms", "0");
        infinispanTest.put("debezium.source.topic.prefix", "testc");
        infinispanTest.put("debezium.source.schema.include.list", "inventory");
        infinispanTest.put("debezium.source.table.include.list", "inventory.customers");

        config = infinispanTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
