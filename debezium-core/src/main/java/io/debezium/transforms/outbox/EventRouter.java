/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;

/**
 * Debezium Outbox Transform Event Router
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    EventRouterDelegate<R> eventRouterDelegate = new EventRouterDelegate<>();

    @Override
    public R apply(R r) {
        return eventRouterDelegate.apply(r, rec -> rec);
    }

    @Override
    public ConfigDef config() {
        return eventRouterDelegate.config();
    }

    @Override
    public void close() {
        eventRouterDelegate.close();
    }

    @Override
    public void configure(Map<String, ?> configMap) {
        eventRouterDelegate.configure(configMap);
    }

    @Override
    public String version() {
        return Module.version();
    }
}
