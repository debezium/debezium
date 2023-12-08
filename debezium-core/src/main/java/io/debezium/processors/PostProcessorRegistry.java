/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors;

import java.util.Collections;
import java.util.List;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.service.Service;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.service.spi.ServiceRegistryAware;
import io.debezium.service.spi.Startable;
import io.debezium.service.spi.Stoppable;

/**
 * Registry of all post processors that are provided by the connector configuration.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class PostProcessorRegistry implements Service, ServiceRegistryAware, Startable, Stoppable {

    @Immutable
    private final List<PostProcessor> processors;
    private ServiceRegistry serviceRegistry;

    public PostProcessorRegistry(List<PostProcessor> processors) {
        if (processors == null) {
            this.processors = Collections.emptyList();
        }
        else {
            this.processors = Collections.unmodifiableList(processors);
        }
    }

    @Override
    public void injectServiceRegistry(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    public void start() {
        for (PostProcessor postProcessor : processors) {
            if (postProcessor instanceof ServiceRegistryAware) {
                ((ServiceRegistryAware) postProcessor).injectServiceRegistry(serviceRegistry);
            }
        }
    }

    @Override
    public void stop() {
        for (PostProcessor postProcessor : processors) {
            postProcessor.close();
        }
    }

    public List<PostProcessor> getProcessors() {
        return this.processors;
    }

}
