/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.configmap;

import java.util.Optional;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;

/**
 * Provider for Kubernetes ConfigMap-based offset storage.
 *
 * @author Debezium Authors
 */
public class ConfigMapOffsetStoreProvider implements OffsetStoreProvider {

    @Override
    public String getName() {
        return "configmap";
    }

    @Override
    public OffsetStore create(Configuration config) {
        return new ConfigMapOffsetStore();
    }

    @Override
    public Optional<String> getOffsetStoreClassName() {
        return Optional.of(ConfigMapOffsetStore.class.getName());
    }
}
