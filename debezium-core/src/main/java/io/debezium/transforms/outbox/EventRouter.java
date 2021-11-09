/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import static java.util.function.Function.identity;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.common.annotation.Incubating;

/**
 * Debezium Outbox Transform Event Router
 *
 * @author Renato mefi (gh@mefi.in)
 */
@Incubating
public class EventRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    EventRouterDelegate<R> eventRouterDelegate = new EventRouterDelegate<>();

    @Override
    public R apply(R r) {
        return eventRouterDelegate.apply(r, identity());
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
}
