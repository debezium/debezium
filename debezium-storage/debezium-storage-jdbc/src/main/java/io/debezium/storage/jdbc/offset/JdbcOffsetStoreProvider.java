/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.offset;

import java.util.Optional;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;

/**
 * Provider for JDBC-based offset storage.
 *
 * @author Debezium Authors
 */
public class JdbcOffsetStoreProvider implements OffsetStoreProvider {

    @Override
    public String getName() {
        return "jdbc";
    }

    @Override
    public OffsetStore create(Configuration config) {
        return new JdbcOffsetBackingStore();
    }

    @Override
    public Optional<String> getOffsetStoreClassName() {
        return Optional.of(JdbcOffsetBackingStore.class.getName());
    }
}
