/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import static io.debezium.config.CommonConnectorConfig.SnapshotQueryMode.CUSTOM;

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
import io.debezium.config.EnumeratedValue;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.spi.SnapshotQuery;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link SnapshotQuery}.
 *
 * @author Mario Fiore Vitale
 */
public class SnapshotQueryProvider extends AbstractSnapshotProvider implements ServiceProvider<SnapshotQuery> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotQueryProvider.class);

    final List<SnapshotQuery> snapshotQueryImplementations;

    public SnapshotQueryProvider() {

        this.snapshotQueryImplementations = StreamSupport.stream(ServiceLoader.load(SnapshotQuery.class).spliterator(), false)
                .collect(Collectors.toList());
    }

    public SnapshotQueryProvider(List<SnapshotQuery> snapshotQueryImplementations) {

        this.snapshotQueryImplementations = snapshotQueryImplementations;
    }

    @Override
    public SnapshotQuery createService(Configuration configuration, ServiceRegistry serviceRegistry) {

        BeanRegistry beanRegistry = serviceRegistry.tryGetService(BeanRegistry.class);
        CommonConnectorConfig commonConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class);

        final EnumeratedValue configuredSnapshotQueryMode = commonConnectorConfig.snapshotQueryMode();
        final String snapshotQueryModeCustomName = commonConnectorConfig.snapshotQueryModeCustomName();

        String snapshotQueryMode;
        Predicate<SnapshotQuery> byNameAndConnectorFilter;
        Predicate<SnapshotQuery> byNameFilter;
        if (CUSTOM.equals(configuredSnapshotQueryMode) && !snapshotQueryModeCustomName.isEmpty()) {
            snapshotQueryMode = snapshotQueryModeCustomName;
            byNameFilter = byNameAndConnectorFilter = snapshotQueryImplementation -> snapshotQueryImplementation.name().equals(snapshotQueryMode);
        }
        else {
            snapshotQueryMode = configuredSnapshotQueryMode.getValue();
            byNameFilter = snapshotQueryImplementation -> snapshotQueryImplementation.name().equals(snapshotQueryMode);
            byNameAndConnectorFilter = byNameFilter.and(snapshotQueryImplementation -> isForCurrentConnector(configuration, snapshotQueryImplementation.getClass()));
        }

        Optional<SnapshotQuery> snapshotQuery = snapshotQueryImplementations.stream()
                .filter(byNameAndConnectorFilter)
                .findAny();

        if (snapshotQuery.isEmpty()) { // Fallback on generic implementation
            snapshotQuery = snapshotQueryImplementations.stream()
                    .filter(byNameFilter)
                    .findAny();
            snapshotQuery.ifPresent(QueryImpl -> LOGGER.warn("Found a not connector specific implementation {} for query mode {}",
                    QueryImpl.getClass().getName(),
                    snapshotQueryMode));
        }

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
