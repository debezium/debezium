/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.lock.NoLockingSupport;
import io.debezium.snapshot.spi.SnapshotLock;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link SnapshotLock}.
 *
 * @author Mario Fiore Vitale
 */
public class SnapshotLockProvider extends AbstractSnapshotProvider implements ServiceProvider<SnapshotLock> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotLockProvider.class);

    final List<SnapshotLock> snapshotLockImplementations;

    public SnapshotLockProvider() {

        this.snapshotLockImplementations = StreamSupport.stream(ServiceLoader.load(SnapshotLock.class).spliterator(), false)
                .collect(Collectors.toList());
    }

    public SnapshotLockProvider(List<SnapshotLock> snapshotLockImplementations) {

        this.snapshotLockImplementations = snapshotLockImplementations;
    }

    @Override
    public SnapshotLock createService(Configuration configuration, ServiceRegistry serviceRegistry) {

        BeanRegistry beanRegistry = serviceRegistry.tryGetService(BeanRegistry.class);

        final CommonConnectorConfig commonConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class);
        final String configuredSnapshotLockingMode = snapshotLockingMode(commonConnectorConfig);
        final String snapshotLockingModeCustomName = commonConnectorConfig.snapshotLockingModeCustomName();

        String snapshotLockMode;
        Predicate<SnapshotLock> byNameAndConnectorFilter;
        Predicate<SnapshotLock> byNameFilter;

        if ("custom".equals(configuredSnapshotLockingMode) && !snapshotLockingModeCustomName.isEmpty()) {
            snapshotLockMode = snapshotLockingModeCustomName;
            byNameAndConnectorFilter = byNameFilter = snapshotLockImplementation -> snapshotLockImplementation.name().equals(snapshotLockMode);
        }
        else {
            snapshotLockMode = configuredSnapshotLockingMode;
            byNameFilter = snapshotLockImplementation -> snapshotLockImplementation.name().equals(snapshotLockMode);
            byNameAndConnectorFilter = byNameFilter.and(snapshotLockImplementation -> isForCurrentConnector(configuration, snapshotLockImplementation.getClass()));
        }

        Optional<? extends SnapshotLock> snapshotLock = snapshotLockImplementations.stream()
                .filter(byNameAndConnectorFilter)
                .findAny();

        if (snapshotLock.isEmpty()) { // Fallback on generic implementation
            snapshotLock = snapshotLockImplementations.stream()
                    .filter(byNameFilter)
                    .findAny();
            snapshotLock.ifPresent(lockImpl -> LOGGER.warn("Found a not connector specific implementation {} for lock mode {}",
                    lockImpl.getClass().getName(),
                    snapshotLockMode));
        }

        return snapshotLock.map(snapshotLockImpl -> {
            snapshotLockImpl.configure(configuration.asMap());
            if (snapshotLockImpl instanceof BeanRegistryAware) {
                ((BeanRegistryAware) snapshotLockImpl).injectBeanRegistry(beanRegistry);
            }
            return snapshotLockImpl;
        })
                .orElseThrow(() -> new DebeziumException(String.format("Unable to find %s snapshot lock mode. Please check your configuration.", snapshotLockMode)));

    }

    @Override
    public Class<SnapshotLock> getServiceClass() {
        return SnapshotLock.class;
    }

    // SnapshotLockingMode differs from different connectors, so it is not moved to CommonConnectorConfig.
    public String snapshotLockingMode(CommonConnectorConfig configuration) {

        if (configuration.getSnapshotLockingMode().isEmpty()) {
            return NoLockingSupport.NO_LOCKING_SUPPORT;
        }

        return configuration.getSnapshotLockingMode().get().getValue();
    }

}
