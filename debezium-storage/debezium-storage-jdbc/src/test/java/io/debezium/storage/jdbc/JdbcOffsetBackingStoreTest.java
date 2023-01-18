/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import static io.debezium.storage.jdbc.JdbcOffsetBackingStore.JDBC_PASSWORD;
import static io.debezium.storage.jdbc.JdbcOffsetBackingStore.JDBC_URI;
import static io.debezium.storage.jdbc.JdbcOffsetBackingStore.JDBC_USER;
import static io.debezium.storage.jdbc.JdbcOffsetBackingStore.OFFSET_STORAGE_TABLE_NAME;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.Callback;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Ismail simsek
 */
public class JdbcOffsetBackingStoreTest {

    private final Map<ByteBuffer, ByteBuffer> firstSet = new HashMap<>();
    private final Map<ByteBuffer, ByteBuffer> secondSet = new HashMap<>();

    JdbcOffsetBackingStore store;
    Map<String, String> props;
    JdbcConfig config;
    File dbFile;

    @Before
    public void setup() throws IOException {
        dbFile = File.createTempFile("test-", "db");
        store = new JdbcOffsetBackingStore();
        props = new HashMap<>();
        props.put(JDBC_URI.name(), "jdbc:sqlite:" + dbFile.getAbsolutePath());
        props.put(JDBC_USER.name(), "user");
        props.put(JDBC_PASSWORD.name(), "pass");
        props.put(OFFSET_STORAGE_TABLE_NAME.name(), "offsets_jdbc");
        props.put(StandaloneConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(StandaloneConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        config = new JdbcConfig(props);
        store.configure(config);
        store.start();

        firstSet.put(store.toByteBuffer("key"), store.toByteBuffer("value"));
        firstSet.put(null, null);
        secondSet.put(store.toByteBuffer("key1secondSet"), store.toByteBuffer("value1secondSet"));
        secondSet.put(store.toByteBuffer("key2secondSet"), store.toByteBuffer("value2secondSet"));
    }

    @After
    public void teardown() {
        dbFile.delete();
    }

    @Test
    public void testInitialize() {
        // multiple initialization should not fail
        // first one should create the table and following ones should use the created table
        store.start();
        store.start();
        store.start();
    }

    @Test
    public void testGetSet() throws Exception {
        Callback<Void> cb = new Callback<Void>() {
            public void onCompletion(Throwable error, Void result) {
                return;
            }
        };
        store.set(firstSet, cb).get();

        Map<ByteBuffer, ByteBuffer> values = store.get(Arrays.asList(store.toByteBuffer("key"), store.toByteBuffer("bad"))).get();
        assertEquals(store.toByteBuffer("value"), values.get(store.toByteBuffer("key")));
        Assert.assertNull(values.get(store.toByteBuffer("bad")));
    }

    @Test
    public void testSaveRestore() throws Exception {
        Callback<Void> cb = new Callback<Void>() {
            public void onCompletion(Throwable error, Void result) {
                return;
            }
        };

        store.set(firstSet, cb).get();
        store.set(secondSet, cb).get();
        store.stop();

        // Restore into a new store mand make sure its correctly reload
        JdbcOffsetBackingStore restore = new JdbcOffsetBackingStore();
        restore.configure(config);
        restore.start();
        Map<ByteBuffer, ByteBuffer> values = restore.get(Collections.singletonList(store.toByteBuffer("key"))).get();
        Map<ByteBuffer, ByteBuffer> values2 = restore.get(Collections.singletonList(store.toByteBuffer("key1secondSet"))).get();
        Map<ByteBuffer, ByteBuffer> values3 = restore.get(Collections.singletonList(store.toByteBuffer("key2secondSet"))).get();
        assertEquals(store.toByteBuffer("value"), values.get(store.toByteBuffer("key")));
        assertEquals(store.toByteBuffer("value1secondSet"), values2.get(store.toByteBuffer("key1secondSet")));
        assertEquals(store.toByteBuffer("value2secondSet"), values3.get(store.toByteBuffer("key2secondSet")));
    }

}
