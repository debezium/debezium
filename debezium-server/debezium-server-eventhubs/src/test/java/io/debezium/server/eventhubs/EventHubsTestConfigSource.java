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
import io.debezium.server.TestDatabase;

public class EventHubsTestConfigSource extends TestConfigSource {

    static final String EVENTHUBS_CONNECTION_STRING_SYSTEM_PROPERTY_NAME = "eventhubs.connection.string";
    static final String EVENTHUBS_NAME_SYSTEM_PROPERTY_NAME = "eventhubs.hub.name";
    static final String CONNECTION_STRING_FORMAT = "%s;EntityPath=%s";

    final Map<String, String> eventHubsTest = new HashMap<>();

    public EventHubsTestConfigSource() {
        // event hubs sink config
        eventHubsTest.put("debezium.sink.type", "eventhubs");
        eventHubsTest.put("debezium.sink.eventhubs.connectionstring", getEventHubsConnectionString());
        eventHubsTest.put("debezium.sink.eventhubs.hubname", getEventHubsName());

        // postgresql source config

        eventHubsTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        eventHubsTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        eventHubsTest.put("debezium.source.offset.flush.interval.ms", "0");
        eventHubsTest.put("debezium.source.database.hostname", TestDatabase.POSTGRES_HOST);
        eventHubsTest.put("debezium.source.database.port", Integer.toString(TestDatabase.POSTGRES_PORT));
        eventHubsTest.put("debezium.source.database.user", TestDatabase.POSTGRES_USER);
        eventHubsTest.put("debezium.source.database.password", TestDatabase.POSTGRES_PASSWORD);
        eventHubsTest.put("debezium.source.database.dbname", TestDatabase.POSTGRES_DBNAME);
        eventHubsTest.put("debezium.source.database.server.name", "testc");
        eventHubsTest.put("debezium.source.schema.whitelist", "inventory");
        eventHubsTest.put("debezium.source.table.whitelist", "inventory.customers");

        config = eventHubsTest;
    }

    public static String getEventHubsConnectionString() {
        return System.getProperty(EVENTHUBS_CONNECTION_STRING_SYSTEM_PROPERTY_NAME);
    }

    public static String getEventHubsName() {
        return System.getProperty(EVENTHUBS_NAME_SYSTEM_PROPERTY_NAME);
    }
}
