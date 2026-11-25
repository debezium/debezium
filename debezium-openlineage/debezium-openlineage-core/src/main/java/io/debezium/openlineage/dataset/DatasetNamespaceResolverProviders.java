/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Loads {@link DatasetNamespaceResolverProvider} implementations via {@link ServiceLoader}.
 */
final class DatasetNamespaceResolverProviders {

    private static final Object LOCK = new Object();
    private static volatile List<DatasetNamespaceResolverProvider> providers;

    private DatasetNamespaceResolverProviders() {
    }

    static Optional<DatasetNamespaceResolverProvider> get(String connectorName) {
        for (DatasetNamespaceResolverProvider provider : allProviders()) {
            if (provider.supports(connectorName)) {
                return Optional.of(provider);
            }
        }
        return Optional.empty();
    }

    private static List<DatasetNamespaceResolverProvider> allProviders() {
        List<DatasetNamespaceResolverProvider> loaded = providers;
        if (loaded == null) {
            synchronized (LOCK) {
                loaded = providers;
                if (loaded == null) {
                    loaded = new ArrayList<>();
                    ServiceLoader.load(DatasetNamespaceResolverProvider.class)
                            .stream()
                            .map(ServiceLoader.Provider::get)
                            .forEach(loaded::add);
                    providers = loaded;
                }
            }
        }
        return loaded;
    }
}
