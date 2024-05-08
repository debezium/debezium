/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.service.Service;
import io.debezium.service.spi.InjectService;
import io.debezium.service.spi.Startable;

/**
 * Registry of all post processors that are provided by the connector configuration.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class PostProcessorRegistry implements Service, Startable, Closeable {

    @Immutable
    private final List<PostProcessor> processors;
    private BeanRegistry beanRegistry;

    public PostProcessorRegistry(List<PostProcessor> processors) {
        if (processors == null) {
            this.processors = Collections.emptyList();
        }
        else {
            this.processors = Collections.unmodifiableList(processors);
        }
    }

    @InjectService
    public void setBeanRegistry(BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override
    public void start() {
        for (PostProcessor postProcessor : processors) {
            if (postProcessor instanceof BeanRegistryAware) {
                ((BeanRegistryAware) postProcessor).injectBeanRegistry(beanRegistry);
            }
        }
    }

    @Override
    public void close() {
        for (PostProcessor postProcessor : processors) {
            postProcessor.close();
        }
    }

    public List<PostProcessor> getProcessors() {
        return this.processors;
    }

}
