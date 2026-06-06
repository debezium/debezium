/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.config.Configuration;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;

public class DebeziumHeaderProducerProvider implements ServiceProvider<DebeziumHeaderProducer> {

    @Override
    public Class<DebeziumHeaderProducer> getServiceClass() {
        return DebeziumHeaderProducer.class;
    }

    @Override
    public DebeziumHeaderProducer createService(Configuration configuration, ServiceRegistry serviceRegistry) {

        BeanRegistry beanRegistry = serviceRegistry.tryGetService(BeanRegistry.class);
        return new DebeziumHeaderProducer(beanRegistry.lookupByName(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, CdcSourceTaskContext.class));
    }
}
