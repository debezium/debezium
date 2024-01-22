/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotLockingMode.CUSTOM;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotLockingMode;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
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
        PostgresConnectorConfig postgresConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, PostgresConnectorConfig.class);

        final SnapshotLockingMode configuredSnapshotLockingMode = postgresConnectorConfig.snapshotLockingMode();
        final String snapshotLockingModeCustomName = postgresConnectorConfig.snapshotLockingModeCustomName();

        String snapshotLockingMode;
        if (CUSTOM.equals(configuredSnapshotLockingMode) && !snapshotLockingModeCustomName.isEmpty()) {
            snapshotLockingMode = snapshotLockingModeCustomName;
        }
        else {
            snapshotLockingMode = configuredSnapshotLockingMode.getValue();
        }

        Optional<SnapshotLock> snapshotLock = StreamSupport.stream(ServiceLoader.load(SnapshotLock.class).spliterator(), false)
                .filter(s -> s.name().equals(snapshotLockingMode))
                .findAny();

        return snapshotLock.map(s -> {
            s.configure(configuration.asMap());
            if (s instanceof BeanRegistryAware) {
                ((BeanRegistryAware) s).injectBeanRegistry(beanRegistry);
            }
            return s;
        })
                .orElseThrow(
                        () -> new DebeziumException(String.format("Unable to find %s snapshot locking mode. Please check your configuration.", snapshotLockingMode)));

    }

    @Override
    public Class<SnapshotLock> getServiceClass() {
        return SnapshotLock.class;
    }

}
