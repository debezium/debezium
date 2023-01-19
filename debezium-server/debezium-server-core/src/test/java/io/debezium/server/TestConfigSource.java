/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.eclipse.microprofile.config.spi.ConfigSource;

import io.debezium.data.Json;
import io.debezium.util.Testing;

/**
 * A config source used during tests. Amended/overridden by values exposed from test lifecycle listeners.
 */
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
        integrationTest.put("debezium.source.topic.prefix", "testc");
        integrationTest.put("debezium.source.schema.include.list", "inventory");
        integrationTest.put("debezium.source.table.include.list", "inventory.customers");

        String format = System.getProperty("test.apicurio.converter.format");
        String formatKey = System.getProperty("debezium.format.key");
        String formatValue = System.getProperty("debezium.format.value");
        String formatHeader = System.getProperty("debezium.format.header", "json");

        if (format != null && format.length() != 0) {
            integrationTest.put("debezium.format.key", format);
            integrationTest.put("debezium.format.value", format);
            integrationTest.put("debezium.format.header", formatHeader);
        }
        else {
            formatKey = (formatKey != null) ? formatKey : Json.class.getSimpleName().toLowerCase();
            formatValue = (formatValue != null) ? formatValue : Json.class.getSimpleName().toLowerCase();
            formatHeader = (formatHeader != null) ? formatHeader : Json.class.getSimpleName().toLowerCase();
            integrationTest.put("debezium.format.key", formatKey);
            integrationTest.put("debezium.format.value", formatValue);
            integrationTest.put("debezium.format.header", formatHeader);
        }

        unitTest.put("debezium.sink.type", "test");
        unitTest.put("debezium.source.connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        unitTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        unitTest.put("debezium.source.offset.flush.interval.ms", "0");
        unitTest.put("debezium.source.file", TEST_FILE_PATH.toAbsolutePath().toString());
        unitTest.put("debezium.source.topic", "topicX");
        unitTest.put("debezium.format.header", formatHeader);
        unitTest.put("debezium.format.schemas.enable", "true");
        unitTest.put("debezium.format.header.schemas.enable", "false");
        unitTest.put("debezium.format.value.schemas.enable", "false");
        unitTest.put("debezium.transforms", "hoist");
        unitTest.put("debezium.transforms.hoist.type", "org.apache.kafka.connect.transforms.HoistField$Value");
        unitTest.put("debezium.transforms.hoist.field", "line");
        unitTest.put("debezium.transforms.hoist.predicate", "topicNameMatch");
        unitTest.put("debezium.predicates", "topicNameMatch");
        unitTest.put("debezium.predicates.topicNameMatch.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
        unitTest.put("debezium.predicates.topicNameMatch.pattern", ".*");

        // DBZ-2622 For testing properties passed via smallrye/microprofile environment variables
        unitTest.put("DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST", "public.table_name");
        unitTest.put("debezium_source_offset_flush_interval_ms_Test", "0");
        unitTest.put("debezium.source.snapshot.select.statement.overrides.public.table_name", "SELECT * FROM table_name WHERE 1>2");
        unitTest.put("debezium.source.database.allowPublicKeyRetrieval", "true");

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

    @Override
    public Set<String> getPropertyNames() {
        return config.keySet();
    }

    public static int waitForSeconds() {
        return 60;
    }
}
