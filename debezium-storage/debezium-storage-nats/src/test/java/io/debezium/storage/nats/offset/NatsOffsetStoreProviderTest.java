/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.offset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;

import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;

/**
 * Verifies that the NATS offset store is discoverable via the
 * {@link OffsetStoreProvider} ServiceLoader mechanism used by the embedded
 * engine in 3.7+.
 *
 * @author Nick Chomey
 */
class NatsOffsetStoreProviderTest {

    @Test
    public void shouldBeDiscoverableViaServiceLoader() {
        Optional<OffsetStoreProvider> provider = ServiceLoader.load(OffsetStoreProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(p -> "nats".equals(p.getName()))
                .findFirst();

        assertTrue(provider.isPresent(), "NATS offset store provider must be registered via ServiceLoader");
        assertEquals(NatsOffsetBackingStore.class.getName(), provider.get().getOffsetStoreClassName().orElse(null));

        OffsetStore store = provider.get().create(null);
        assertThat(store).isInstanceOf(NatsOffsetBackingStore.class);
    }
}