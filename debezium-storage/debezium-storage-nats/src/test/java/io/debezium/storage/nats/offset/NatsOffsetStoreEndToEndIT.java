/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.offset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStorageReader;
import io.debezium.spi.storage.OffsetStorageWriter;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;
import io.debezium.storage.nats.NatsCommonConfig;
import io.debezium.storage.nats.NatsContainer;
import io.debezium.util.Collect;

/**
 * End-to-end test of the offset storage path the embedded engine uses:
 * ServiceLoader provider discovery, {@code configure(Configuration)},
 * then reads and writes through {@link OffsetStorageReader} and
 * {@link OffsetStorageWriter} with the engine's JSON key format.
 *
 * @author Nick Chomey
 */
@Testcontainers
class NatsOffsetStoreEndToEndIT {

    @Container
    @SuppressWarnings("resource")
    public NatsContainer natsContainer = new NatsContainer();

    private String natsUrl;
    private OffsetStore store;

    @BeforeEach
    public void setUp() {
        natsUrl = natsContainer.getServerUrl();
    }

    @AfterEach
    public void tearDown() {
        if (store != null) {
            store.stop();
        }
    }

    @Test
    @Timeout(30)
    public void shouldRoundTripOffsetsThroughReaderAndWriter() throws Exception {
        // Discover the provider the way the engine does
        OffsetStoreProvider provider = ServiceLoader.load(OffsetStoreProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(p -> "nats".equals(p.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("NATS offset store provider not found"));

        // Configure with the engine's key layout: offset.storage.nats.*
        Map<String, String> props = new HashMap<>();
        props.put(NatsOffsetBackingStoreConfig.PROP_PREFIX + NatsCommonConfig.NATS_URL.name(), natsUrl);
        props.put(NatsOffsetBackingStoreConfig.PROP_PREFIX + NatsOffsetBackingStoreConfig.PROP_BUCKET_NAME.name(),
                "e2e-offsets");
        Configuration config = Configuration.from(props);

        store = provider.create(config);
        store.configure(config);
        store.start();

        OffsetStorageWriter writer = store.createWriter("test-namespace");
        OffsetStorageReader reader = store.createReader("test-namespace");

        // Write offsets the way the engine does
        Map<String, Object> partition = Collect.linkMapOf("server", "test-server");
        Map<String, Object> offset = Collect.linkMapOf("file", "test.log", "position", 100L);
        writer.offset(partition, offset);
        writer.beginFlush(10, TimeUnit.SECONDS);
        writer.doFlush(null).get(10, TimeUnit.SECONDS);

        // Read them back. Note: JSON round-trip turns Long 100L into Integer
        // 100, so compare field-wise with Number coercion.
        Map<String, Object> read = reader.offset(partition);
        assertEquals("test.log", read.get("file"));
        assertEquals(100L, ((Number) read.get("position")).longValue());

        // A partition that was never written must return null
        assertNull(reader.offset(Collect.linkMapOf("server", "other-server")));
    }
}
