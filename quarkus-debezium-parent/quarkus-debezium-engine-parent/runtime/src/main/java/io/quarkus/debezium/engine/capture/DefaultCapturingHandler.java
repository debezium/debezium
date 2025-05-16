/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.RecordChangeEvent;
import io.quarkus.debezium.engine.FullyQualifiedTableNameResolver;

class DefaultCapturingHandler implements DebeziumCapturingHandler {
    private final CapturingInvokerRegistry registry;
    private final FullyQualifiedTableNameResolver resolver;

    DefaultCapturingHandler(CapturingInvokerRegistry registry, FullyQualifiedTableNameResolver resolver) {
        this.registry = registry;
        this.resolver = resolver;
    }

    @Override
    public void accept(RecordChangeEvent<SourceRecord> event) {
        registry.get(resolver.resolve(event))
                .capture(event);
    }

}
