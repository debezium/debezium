/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.stream.Collectors;

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
        Map<String, CapturingEventDeserializer<?, SourceRecord>> deserializers = configuration
                .capturing()
                .values()
                .stream()
                .collect(Collectors.toMap(DebeziumEngineConfiguration.Capturing::destination, c -> getDeserializer(c.deserializer())));

        return deserializers::get;
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
