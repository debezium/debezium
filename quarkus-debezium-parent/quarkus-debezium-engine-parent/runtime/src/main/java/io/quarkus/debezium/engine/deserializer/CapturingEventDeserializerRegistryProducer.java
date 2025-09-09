/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

import static java.util.stream.Collectors.toMap;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.debezium.engine.CapturingEventDeserializer;
import io.quarkus.debezium.engine.SourceRecordDeserializer;

public class CapturingEventDeserializerRegistryProducer {

    private final Logger LOGGER = LoggerFactory.getLogger(CapturingEventDeserializerRegistryProducer.class);
    private final JsonConverter converter = new JsonConverter();

    @Produces
    @Singleton
    public CapturingEventDeserializerRegistry<SourceRecord> produce(DebeziumEngineConfiguration configuration) {
        Map<String, CapturingEventDeserializer<?, SourceRecord>> nestedDeserializers = configuration
                .capturing()
                .values()
                .stream()
                .flatMap(a -> a.deserializers().values().stream())
                .collect(toMap(DebeziumEngineConfiguration.DeserializerConfiguration::destination,
                        config -> getDeserializer(config.deserializer())));

        Map<String, CapturingEventDeserializer<?, SourceRecord>> deserializers = configuration
                .capturing()
                .values()
                .stream()
                .filter(config -> config.deserializer().isPresent() && config.destination().isPresent())
                .collect(toMap(config -> config.destination().get(), config -> getDeserializer(config.deserializer().get())));

        deserializers.putAll(nestedDeserializers);

        LOGGER.trace("Collecting deserializers: {}", deserializers.keySet());

        return new MutableCapturingEventDeserializerRegistry<>() {

            @Override
            public void register(String identifier, Deserializer<?> deserializer) {
                deserializers.put(identifier, new SourceRecordDeserializer<>(deserializer, converter));
            }

            @Override
            public void unregister(String identifier) {
                deserializers.remove(identifier);
            }

            @Override
            public CapturingEventDeserializer<?, SourceRecord> get(String identifier) {
                return deserializers.get(identifier);
            }
        };
    }

    private CapturingEventDeserializer<?, SourceRecord> getDeserializer(String deserializer) {
        try {
            return new SourceRecordDeserializer<>((Deserializer<?>) Class
                    .forName(deserializer, true, Thread.currentThread().getContextClassLoader()).getDeclaredConstructor().newInstance(), converter);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
