/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.RecordChangeEvent;

public class DefaultCapturingHandler implements DebeziumCapturingHandler {
    private final CapturingInvokerRegistry registry;

    public DefaultCapturingHandler(CapturingInvokerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void accept(RecordChangeEvent<SourceRecord> event) {
        SourceRecord record = event.record();
        Struct payload = (Struct) record.value();
        String table = ((Struct) payload.get("source")).getString("table");

        CapturingInvoker invoker = registry.get(table);

        invoker.capture(event);
    }

}
