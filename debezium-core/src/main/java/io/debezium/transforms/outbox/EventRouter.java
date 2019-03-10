/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

/**
 * Debezium Outbox Transform Event Router
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public R apply(R r) {
        return null;
    }

    @Override
    public ConfigDef config() {
        return EventRouterConfigDefinition.configDef();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
