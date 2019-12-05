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

    public static <T> Serde<T> payloadJson(Class<T> objectType) {
        return new JsonSerde<>(objectType);
    }
}
