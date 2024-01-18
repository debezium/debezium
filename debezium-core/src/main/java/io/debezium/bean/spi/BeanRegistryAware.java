/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bean.spi;

/**
 * Allows the injection of the {@link BeanRegistry} during service configuration.
 *
 * @author Chris Cranford
 */
public interface BeanRegistryAware {
    /**
     * Callback to inject the bean registry.
     *
     * @param beanRegistry the bean registry
     */
    void injectBeanRegistry(BeanRegistry beanRegistry);
}
