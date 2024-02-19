/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.mode;

import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;

public class BeanAwareSnapshotter implements BeanRegistryAware {
    protected BeanRegistry beanRegistry;

    @Override
    public void injectBeanRegistry(BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

}
