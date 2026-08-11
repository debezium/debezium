/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.monitor;

import java.time.temporal.ChronoUnit;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link OffsetActivityMonitorService}.
 *
 * @author Chris Cranford
 */
public class OffsetActivityMonitorServiceProvider implements ServiceProvider<OffsetActivityMonitorService> {

    @Override
    public OffsetActivityMonitorService createService(Configuration configuration, ServiceRegistry serviceRegistry) {
        return new DefaultOffsetActivityMonitorService(
                configuration.getDuration(CommonConnectorConfig.OFFSET_ACTIVITY_MONITOR_INTERVAL_MS, ChronoUnit.MILLIS));
    }

    @Override
    public Class<OffsetActivityMonitorService> getServiceClass() {
        return OffsetActivityMonitorService.class;
    }
}