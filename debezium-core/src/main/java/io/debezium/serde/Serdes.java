/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.serde;

import org.apache.kafka.common.serialization.Serde;

import io.debezium.annotation.Incubating;
import io.debezium.serde.json.JsonSerde;

/**
 * A factory class for Debezium provided serializers/deserializers.
 *
 * @author Jiri Pechanec
 *
 */
@Incubating
public class Serdes {

    /**
     * Provides a {@link Serde} implementation that maps JSON Debezium message into a {@code T} Java class.<br />
     * When used as key deserializer then the key fields are directly mapped into Java object key.<br />
     * When used as value deserializer its behaviour is driven by value of {@code from.field} config option
     * <ul>
     * <li>not set - maps complete message envelope</li>
     * <li>{@code before/after} - extracts the given field from the envelope and maps it
     * </ul>
     * If schema is enabled then the serde will extract the {@code payload} field to get the envelope and applies
     * the rules above.
     * 
     * @param <T> type to which JSON is mapped
     * @param objectType type to which JSON is mapped
     * @return serializer/deserializer to convert JSON ro/from Java class
     */
    public static <T> Serde<T> payloadJson(Class<T> objectType) {
        return new JsonSerde<>(objectType);
    }
}
