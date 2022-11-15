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

import io.debezium.outbox.quarkus.internal.DebeziumOutboxHandler;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class MyService {
    // @Inject
    // EventBus bus;
    @Inject
    DebeziumOutboxHandler handler;

    public Uni<Object> doSomething() {
        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe"); // illustrates additional field with no converter
        values.put("name_upper", "John Doe"); // illustrates additional field with converter
        values.put("name_no_columndef", "Jane Doe"); // illustrates default behavior with no column definition specified

        return handler.persistToOutbox(new MyOutboxEvent(values));
        // MyOutboxEvent event1 = new MyOutboxEvent(values);
        //
        // DebeziumCustomCodec myCodec = new DebeziumCustomCodec();
        // bus.registerCodec(myCodec);
        //
        // DeliveryOptions options = new DeliveryOptions().setCodecName(myCodec.name());
        //
        // return bus.<MyOutboxEvent> request("debezium-outbox", event1, options)
        // .onItem().transform(message -> message.body());
    }
}
