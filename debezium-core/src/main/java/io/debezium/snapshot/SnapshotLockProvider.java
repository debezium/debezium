/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

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
public class SnapshotLockProvider implements ServiceProvider<SnapshotLock> {

    @Override
    public SnapshotLock createService(Configuration configuration, ServiceRegistry serviceRegistry) {

        BeanRegistry beanRegistry = serviceRegistry.tryGetService(BeanRegistry.class);

        final CommonConnectorConfig commonConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class);
        final String configuredSnapshotLockingMode = snapshotLockingMode(commonConnectorConfig);
        final String snapshotLockingModeCustomName = commonConnectorConfig.snapshotLockingModeCustomName();

        String snapshotLockMode;
        if ("custom".equals(configuredSnapshotLockingMode) && !snapshotLockingModeCustomName.isEmpty()) {
            snapshotLockMode = snapshotLockingModeCustomName;
        }
        else {
            snapshotLockMode = configuredSnapshotLockingMode;
        }

        Optional<SnapshotLock> snapshotLock = StreamSupport.stream(ServiceLoader.load(SnapshotLock.class).spliterator(), false)
                .filter(s -> s.name().equalsIgnoreCase(snapshotLockMode))
                .findAny();

        return snapshotLock.map(s -> {
            s.configure(configuration.asMap());
            if (s instanceof BeanRegistryAware) {
                ((BeanRegistryAware) s).injectBeanRegistry(beanRegistry);
            }
            return s;
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
