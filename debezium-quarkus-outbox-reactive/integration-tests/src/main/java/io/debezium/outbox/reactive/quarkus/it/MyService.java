/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.it;

import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.debezium.outbox.reactive.quarkus.internal.DebeziumOutboxHandler;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class MyService {

    @Inject
    DebeziumOutboxHandler handler;

    public Uni<Object> doSomething() {
        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe"); // illustrates additional field with no converter
        values.put("name_upper", "John Doe"); // illustrates additional field with converter
        values.put("name_no_columndef", "Jane Doe"); // illustrates default behavior with no column definition specified
        return handler.persistToOutbox(new MyOutboxEvent(values));
    }
}
