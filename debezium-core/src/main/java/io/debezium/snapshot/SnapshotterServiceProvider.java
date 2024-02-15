/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.spi.SnapshotLock;
import io.debezium.snapshot.spi.SnapshotQuery;
import io.debezium.spi.snapshot.Snapshotter;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link SnapshotterService}.
 *
 * @author Mario Fiore Vitale
 */
public abstract class SnapshotterServiceProvider implements ServiceProvider<SnapshotterService> {

    @Override
    public SnapshotterService createService(Configuration configuration, ServiceRegistry serviceRegistry) {

        final BeanRegistry beanRegistry = serviceRegistry.tryGetService(BeanRegistry.class);
        final CommonConnectorConfig commonConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class);

        final String configuredSnapshotMode = snapshotMode(beanRegistry);
        final String snapshotModeCustomName = commonConnectorConfig.getSnapshotModeCustomName();

        String snapshotMode;
        if ("custom".equals(configuredSnapshotMode) && !snapshotModeCustomName.isEmpty()) {
            snapshotMode = snapshotModeCustomName;
        }
        else {
            snapshotMode = configuredSnapshotMode;
        }

        List<Snapshotter> snapshotters = StreamSupport.stream(ServiceLoader.load(Snapshotter.class).spliterator(), false)
                .filter(s -> s.name().equals(snapshotMode))
                .collect(Collectors.toList());

        if (snapshotters.isEmpty()) {
            throw new DebeziumException(String.format("Unable to find %s snapshotter. Please check your configuration.", snapshotMode));
        }

        if (snapshotters.size() > 1) {
            throw new DebeziumException("Found multiple implementation for {} snapshotter. Please verify your configuration.");
        }

        final SnapshotQuery snapshotQueryService = serviceRegistry.tryGetService(SnapshotQuery.class);
        final SnapshotLock snapshotLockService = serviceRegistry.tryGetService(SnapshotLock.class);
        final Snapshotter snapshotter = snapshotters.get(0);

        return getSnapshotterService(configuration, snapshotter, beanRegistry, snapshotQueryService, snapshotLockService);
    }

    // This is required for DebeziumServer since it loads all connectors and until all modes will be moved into the core (if possible)
    private boolean isForCurrentConnector(Configuration configuration, Snapshotter s) {

        return s.getClass().getCanonicalName().contains(getConnectorClassPackage(configuration));
    }

    private String getConnectorClassPackage(Configuration config) {

        try {
            return Class.forName(config.getString("connector.class")).getPackageName();
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static SnapshotterService getSnapshotterService(Configuration configuration, Snapshotter s, BeanRegistry beanRegistry, SnapshotQuery snapshotQueryService,
                                                            SnapshotLock snapshotLockService) {
        s.configure(configuration.asMap());
        if (s instanceof BeanRegistryAware) {
            ((BeanRegistryAware) s).injectBeanRegistry(beanRegistry);
        }
        return new SnapshotterService(s, snapshotQueryService, snapshotLockService);
    }

    @Override
    public Class<SnapshotterService> getServiceClass() {
        return SnapshotterService.class;
    }

    // TODO this could be delete after DBZ-7308 if all modes will be effectively available to all connectors and
    // SnapshotMode enum moved into CommonConnectorConfig
    public abstract String snapshotMode(BeanRegistry beanRegistry);

}
