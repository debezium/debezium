package io.debezium.server.redis;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTestProfile;

public class RedisStreamTestProfile implements QuarkusTestProfile {

    public static final String OFFSETS_FILE = "file-connector-offsets.txt";
    public static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath(OFFSETS_FILE).toAbsolutePath();
    public static final String OFFSET_STORAGE_FILE_FILENAME_CONFIG = "offset.storage.file.filename";

    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<String, String>();
        config.put("debezium.source." + OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        return config;
    }

}
