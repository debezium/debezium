/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class EventHubsTestConfigSource extends TestConfigSource {

    static final String EVENTHUBS_CONNECTION_STRING_SYSTEM_PROPERTY_NAME = "eventhubs.connection.string";
    static final String EVENTHUBS_NAME_SYSTEM_PROPERTY_NAME = "eventhubs.hub.name";
    static final String CONNECTION_STRING_FORMAT = "%s;EntityPath=%s";

    public EventHubsTestConfigSource() {
        Map<String, String> eventHubsTest = new HashMap<>();

        // event hubs sink config
        eventHubsTest.put("debezium.sink.type", "eventhubs");
        eventHubsTest.put("debezium.sink.eventhubs.connectionstring", getEventHubsConnectionString());
        eventHubsTest.put("debezium.sink.eventhubs.hubname", getEventHubsName());

        // postgresql source config

        eventHubsTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        eventHubsTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        eventHubsTest.put("debezium.source.offset.flush.interval.ms", "0");
        eventHubsTest.put("debezium.source.database.server.name", "testc");
        eventHubsTest.put("debezium.source.schema.include.list", "inventory");
        eventHubsTest.put("debezium.source.table.include.list", "inventory.customers");

        config = eventHubsTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }

    public static String getEventHubsConnectionString() {
        return System.getProperty(EVENTHUBS_CONNECTION_STRING_SYSTEM_PROPERTY_NAME);
    }

    public static String getEventHubsName() {
        return System.getProperty(EVENTHUBS_NAME_SYSTEM_PROPERTY_NAME);
    }
}
