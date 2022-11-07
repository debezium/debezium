/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTestProfile;

public class RedisStreamTestProfile implements QuarkusTestProfile {

    public static final String OFFSETS_FILE = "file-connector-offsets.txt";
    public static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath(OFFSETS_FILE).toAbsolutePath();
    public static final String OFFSET_STORAGE_FILE_FILENAME_CONFIG = "offset.storage.file.filename";

    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(new TestResourceEntry(PostgresTestResourceLifecycleManager.class));
    }

    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<String, String>();
        config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        config.put("debezium.source." + OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        config.put("debezium.sink.redis.memory.threshold.percentage", "0");
        return config;
    }

}
