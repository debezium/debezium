/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.debezium.engine.CapturingEventDeserializer;
import io.quarkus.debezium.engine.SourceRecordDeserializer;

public class CapturingEventDeserializerRegistryProducer {

    private final JsonConverter converter = new JsonConverter();

    @Produces
    @Singleton
    public CapturingEventDeserializerRegistry<SourceRecord> produce(DebeziumEngineConfiguration configuration) {
        HashMap<String, CapturingEventDeserializer<?, SourceRecord>> registry = new HashMap<>();

        configuration
                .capturing()
                .values()
                .stream()
                .filter(c -> c.deserializer().isPresent() && c.destination().isPresent())
                .forEach(a -> registry.put(a.destination().get(), getDeserializer(a.deserializer().get())));

        return new MutableCapturingEventDeserializerRegistry<>() {
            @Override
            public void register(String identifier, Deserializer<?> deserializer) {
                registry.put(identifier, new SourceRecordDeserializer<>(deserializer, converter));
            }

            @Override
            public void unregister(String identifier) {
                registry.remove(identifier);
            }

            @Override
            public CapturingEventDeserializer<?, SourceRecord> get(String identifier) {
                return registry.get(identifier);
            }
        };
    }

    private CapturingEventDeserializer<?, SourceRecord> getDeserializer(String deserializer) {
        try {
            return new SourceRecordDeserializer<>((Deserializer<?>) Class.forName(deserializer).getDeclaredConstructor().newInstance(), converter);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
