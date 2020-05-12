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

    public static final String KINESIS_REGION = "eu-central-1";
    public static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath("file-connector-offsets.txt").toAbsolutePath();
    public static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("file-connector-input.txt").toAbsolutePath();

    final Map<String, String> integrationTest = new HashMap<>();
    final Map<String, String> e2eTest = new HashMap<>();
    final Map<String, String> unitTest = new HashMap<>();
    final Map<String, String> config;

    public TestConfigSource() {
        e2eTest.put("debezium.consumer", "kinesis");
        e2eTest.put("kinesis.region", KINESIS_REGION);
        e2eTest.put("debezium.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        e2eTest.put("debezium." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        e2eTest.put("debezium.offset.flush.interval.ms", "0");
        e2eTest.put("debezium.database.hostname", TestDatabase.POSTGRES_HOST);
        e2eTest.put("debezium.database.port", Integer.toString(TestDatabase.POSTGRES_PORT));
        e2eTest.put("debezium.database.user", TestDatabase.POSTGRES_USER);
        e2eTest.put("debezium.database.password", TestDatabase.POSTGRES_PASSWORD);
        e2eTest.put("debezium.database.dbname", TestDatabase.POSTGRES_DBNAME);
        e2eTest.put("debezium.database.server.name", "testc");
        e2eTest.put("debezium.schema.whitelist", "inventory");
        e2eTest.put("debezium.table.whitelist", "inventory.customers");

        integrationTest.put("debezium.consumer", "test");
        integrationTest.put("debezium.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        integrationTest.put("debezium." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        integrationTest.put("debezium.offset.flush.interval.ms", "0");
        integrationTest.put("debezium.database.hostname", TestDatabase.POSTGRES_HOST);
        integrationTest.put("debezium.database.port", Integer.toString(TestDatabase.POSTGRES_PORT));
        integrationTest.put("debezium.database.user", TestDatabase.POSTGRES_USER);
        integrationTest.put("debezium.database.password", TestDatabase.POSTGRES_PASSWORD);
        integrationTest.put("debezium.database.dbname", TestDatabase.POSTGRES_DBNAME);
        integrationTest.put("debezium.database.server.name", "testc");
        integrationTest.put("debezium.schema.whitelist", "inventory");
        integrationTest.put("debezium.table.whitelist", "inventory.customers");

        unitTest.put("debezium.consumer", "test");
        unitTest.put("debezium.connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        unitTest.put("debezium." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        unitTest.put("debezium.offset.flush.interval.ms", "0");
        unitTest.put("debezium.file", TEST_FILE_PATH.toAbsolutePath().toString());
        unitTest.put("debezium.topic", "topicX");
        unitTest.put("debezium.converter.schemas.enable", "false");
        unitTest.put("debezium.transforms", "hoist");
        unitTest.put("debezium.transforms.hoist.type", "org.apache.kafka.connect.transforms.HoistField$Value");
        unitTest.put("debezium.transforms.hoist.field", "line");

        config = isItTest() ? integrationTest : (isE2eTest() ? e2eTest : unitTest);
    }

    public static boolean isItTest() {
        return "IT".equals(System.getProperty("test.type"));
    }

    public static boolean isE2eTest() {
        return "E2E".equals(System.getProperty("test.type"));
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
