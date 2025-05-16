/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.RecordChangeEvent;

public class CapturingInvokerRegistry {
    private static final Logger logger = LoggerFactory.getLogger(CapturingInvokerRegistry.class);

    Map<String, CapturingInvoker> invokers;

    public CapturingInvokerRegistry(Map<String, CapturingInvoker> invokers) {
        this.invokers = invokers;
    }

    public CapturingInvoker get(String table) {
        return invokers.getOrDefault(table, noOpInvoker(table));
    }

    private CapturingInvoker noOpInvoker(String table) {
        return new CapturingInvoker() {
            @Override
            public void capture(RecordChangeEvent<SourceRecord> event) {
                logger.warn("table {} not assigned to any handler", table);
            }

            @Override
            public String getTable() {
                return table;
            }
        };
    }
}
