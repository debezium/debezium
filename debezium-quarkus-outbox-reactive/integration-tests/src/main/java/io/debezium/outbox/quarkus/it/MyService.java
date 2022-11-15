/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.debezium.outbox.quarkus.XportedEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.eventbus.EventBus;

@ApplicationScoped
public class MyService {
    @Inject
    EventBus bus;
    // @Inject
    // Event<ExportedEvent<?, ?>> event;

    public Uni<Object> doSomething() {
        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe"); // illustrates additional field with no converter
        values.put("name_upper", "John Doe"); // illustrates additional field with converter
        values.put("name_no_columndef", "Jane Doe"); // illustrates default behavior with no column definition specified
        System.out.println("@@@@@@@@@@@@@@@@@@@@ myservice@@@@@@@@@@@@@@@@" + values);
        MyOutboxEvent event1 = new MyOutboxEvent(values);
        // return Uni.createFrom().nullItem();
        // XportedEvent event = new XportedEvent();
        // event.setAggregateId(1L);
        // event.setAggregateType("MyOutboxEvent");
        // event.setType("SomeType");
        // event.setTimestamp(Instant.now());
        // event.setPayload("Some amazing payload");
        // event.setAdditionalValues(values);
        return bus.<XportedEvent> request("debezium-outbox", new XportedEvent(event1))
                .onItem().transform(message -> message.body());
    }
}
