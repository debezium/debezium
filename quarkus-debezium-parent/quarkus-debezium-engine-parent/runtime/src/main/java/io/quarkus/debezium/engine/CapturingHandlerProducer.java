/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.RecordChangeEvent;

public class CapturingHandlerProducer {

    @Inject
    Instance<CapturingInvoker> invokers;

    @Produces
    @Singleton
    public DefaultCapturingHandler produce() {
        CapturingInvokerRegistry capturingInvokerRegistry = new CapturingInvokerRegistry(invokers
                .stream()
                .collect(Collectors.toMap(CapturingInvoker::getFullyQualifiedTableName, Function.identity())));

        return new DefaultCapturingHandler(capturingInvokerRegistry, new FullyQualifiedTableNameResolver() {
            @Override
            public String resolve(RecordChangeEvent<SourceRecord> event) {
                SourceRecord record = event.record();
                Struct payload = (Struct) record.value();
                String table = ((Struct) payload.get("source")).getString("table");
                String schema = ((Struct) payload.get("source")).getString("schema");

                return schema + "." + table;
            }
        });
    }
}
