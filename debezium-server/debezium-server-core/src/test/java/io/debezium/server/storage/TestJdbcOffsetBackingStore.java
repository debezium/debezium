package io.debezium.server.storage;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.Callback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.debezium.server.storage.JdbcOffsetBackingStore.*;
import static org.junit.Assert.assertEquals;

public class TestJdbcOffsetBackingStore {

    private final Map<ByteBuffer, ByteBuffer> firstSet = new HashMap<>();

    JdbcOffsetBackingStore store;
    Map<String, String> props;
    JdbcConfig config;
    String dbFile = "/tmp/test.db";

    @Before
    public void setup() {
        store = new JdbcOffsetBackingStore();
        props = new HashMap<>();
        props.put(JDBC_URI.name(), "jdbc:sqlite:"+dbFile);
        props.put(JDBC_USER.name(), "user");
        props.put(JDBC_PASSWORD.name(), "pass");
        props.put(StandaloneConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(StandaloneConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        config = new JdbcConfig(props);
        store.configure(config);
        store.start();

        firstSet.put(store.toByteBuffer("key"), store.toByteBuffer("value"));
        firstSet.put(null, null);
    }

    @After
    public void teardown() throws IOException {
        File file = new File(dbFile);
        Files.deleteIfExists(file.toPath());
    }

    @Test
    public void testGetSet() throws Exception {
        Callback<Void> cb = new Callback<Void>() {
            public void onCompletion(Throwable error, Void result) {
                return;
            }
        };
        store.start();
        store.start();
        store.start();
        store.set(firstSet, cb).get();

        Map<ByteBuffer, ByteBuffer> values = store.get(Arrays.asList(store.toByteBuffer("key"), store.toByteBuffer("bad"))).get();
        assertEquals(store.toByteBuffer("value"), values.get(store.toByteBuffer("key")));
        assertEquals(null, values.get(store.toByteBuffer("bad")));
    }

    @Test
    public void testSaveRestore() throws Exception {
        Callback<Void> cb = new Callback<Void>() {
            public void onCompletion(Throwable error, Void result) {
                return;
            }
        };

        store.set(firstSet, cb).get();
        store.stop();

        // Restore into a new store to ensure correct reload from scratch
        JdbcOffsetBackingStore restore = new JdbcOffsetBackingStore();
        restore.configure(config);
        restore.start();
        Map<ByteBuffer, ByteBuffer> values = restore.get(Arrays.asList(store.toByteBuffer("key"))).get();
        assertEquals(store.toByteBuffer("value"), values.get(store.toByteBuffer("key")));
    }

}
