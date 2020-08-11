/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.eclipse.microprofile.config.spi.ConfigSource;

import io.debezium.util.Testing;

public class TestConfigSource implements ConfigSource {

    public static final String OFFSETS_FILE = "file-connector-offsets.txt";
    public static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath(OFFSETS_FILE).toAbsolutePath();
    public static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("file-connector-input.txt").toAbsolutePath();

    final Map<String, String> integrationTest = new HashMap<>();
    final Map<String, String> kinesisTest = new HashMap<>();
    final Map<String, String> pubsubTest = new HashMap<>();
    final Map<String, String> unitTest = new HashMap<>();
    protected Map<String, String> config;

    public TestConfigSource() {
        integrationTest.put("debezium.sink.type", "test");
        integrationTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        integrationTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        integrationTest.put("debezium.source.offset.flush.interval.ms", "0");
        integrationTest.put("debezium.source.database.hostname", TestDatabase.POSTGRES_HOST);
        integrationTest.put("debezium.source.database.port", Integer.toString(TestDatabase.POSTGRES_PORT));
        integrationTest.put("debezium.source.database.user", TestDatabase.POSTGRES_USER);
        integrationTest.put("debezium.source.database.password", TestDatabase.POSTGRES_PASSWORD);
        integrationTest.put("debezium.source.database.dbname", TestDatabase.POSTGRES_DBNAME);
        integrationTest.put("debezium.source.database.server.name", "testc");
        integrationTest.put("debezium.source.schema.include.list", "inventory");
        integrationTest.put("debezium.source.table.include.list", "inventory.customers");

        unitTest.put("debezium.sink.type", "test");
        unitTest.put("debezium.source.connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        unitTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        unitTest.put("debezium.source.offset.flush.interval.ms", "0");
        unitTest.put("debezium.source.file", TEST_FILE_PATH.toAbsolutePath().toString());
        unitTest.put("debezium.source.topic", "topicX");
        unitTest.put("debezium.format.schemas.enable", "true");
        unitTest.put("debezium.format.value.schemas.enable", "false");
        unitTest.put("debezium.transforms", "hoist");
        unitTest.put("debezium.transforms.hoist.type", "org.apache.kafka.connect.transforms.HoistField$Value");
        unitTest.put("debezium.transforms.hoist.field", "line");

        if (isItTest()) {
            config = integrationTest;
        }
        else {
            config = unitTest;
        }
    }

    public static boolean isItTest() {
        return "IT".equals(System.getProperty("test.type"));
    }

    @Override
    public Map<String, String> getProperties() {
        return config;
    }

    @Override
    public String getValue(String propertyName) {
        return config.get(propertyName);
    }

    @Override
    public String getName() {
        return "test";
    }

    public static int waitForSeconds() {
        return 60;
    }
}
