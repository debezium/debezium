/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import io.debezium.outbox.quarkus.ExportedEvent;

@ApplicationScoped
public class MyService {

    @Inject
    Event<ExportedEvent<?, ?>> event;

    @Transactional
    public void doSomething() {
        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe"); // illustrates additional field with no converter
        values.put("name_upper", "John Doe"); // illustrates additional field with converter
        values.put("name_no_columndef", "Jane Doe"); // illustrates default behavior with no column definition specified
        event.fire(new MyOutboxEvent(values));
    }
}
