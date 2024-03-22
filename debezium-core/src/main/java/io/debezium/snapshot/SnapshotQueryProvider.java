/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import static io.debezium.config.CommonConnectorConfig.SnapshotQueryMode.CUSTOM;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.SnapshotQueryMode;
import io.debezium.config.Configuration;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.spi.SnapshotQuery;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link SnapshotQuery}.
 *
 * @author Mario Fiore Vitale
 */
public class SnapshotQueryProvider implements ServiceProvider<SnapshotQuery> {

    @Override
    public SnapshotQuery createService(Configuration configuration, ServiceRegistry serviceRegistry) {

        BeanRegistry beanRegistry = serviceRegistry.tryGetService(BeanRegistry.class);
        CommonConnectorConfig commonConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class);

        final SnapshotQueryMode configuredSnapshotQueryMode = commonConnectorConfig.snapshotQueryMode();
        final String snapshotQueryModeCustomName = commonConnectorConfig.snapshotQueryModeCustomName();

        String snapshotQueryMode;
        if (CUSTOM.equals(configuredSnapshotQueryMode) && !snapshotQueryModeCustomName.isEmpty()) {
            snapshotQueryMode = snapshotQueryModeCustomName;
        }
        else {
            snapshotQueryMode = configuredSnapshotQueryMode.getValue();
        }

        Optional<SnapshotQuery> snapshotQuery = StreamSupport.stream(ServiceLoader.load(SnapshotQuery.class).spliterator(), false)
                .filter(s -> s.name().equals(snapshotQueryMode))
                .findAny();

        return snapshotQuery.map(s -> {
            s.configure(configuration.asMap());
            if (s instanceof BeanRegistryAware) {
                ((BeanRegistryAware) s).injectBeanRegistry(beanRegistry);
            }
            return s;
        })
                .orElseThrow(() -> new DebeziumException(String.format("Unable to find %s snapshot query mode. Please check your configuration.", snapshotQueryMode)));

    }

    @Override
    public Class<SnapshotQuery> getServiceClass() {
        return SnapshotQuery.class;
    }

}
