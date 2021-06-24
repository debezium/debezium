/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import javax.transaction.Transactional;

import io.debezium.outbox.quarkus.ExportedEvent;

@ApplicationScoped
public class MyService {

    @Inject
    Event<ExportedEvent<?, ?>> event;

    @Transactional
    public void doSomething() {
        event.fire(new MyOutboxEvent());
    }
}
