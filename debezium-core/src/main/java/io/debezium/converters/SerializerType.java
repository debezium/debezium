/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A set of available serializer types for CloudEvents or the data attribute of CloudEvents.
 */
public enum SerializerType {

    /**
     * Using JSON as serialization format
     */
    JSON,

    /**
     * Using Avro as serialization format
     */
    AVRO;

    private static final Map<String, SerializerType> NAME_TO_TYPE;

    static {
        SerializerType[] types = SerializerType.values();
        Map<String, SerializerType> nameToType = new HashMap<>(types.length);
        for (SerializerType type : types) {
            nameToType.put(type.name, type);
        }
        NAME_TO_TYPE = Collections.unmodifiableMap(nameToType);
    }

    public static SerializerType withName(String name) {
        if (name == null) {
            return null;
        }
        return NAME_TO_TYPE.get(name.toLowerCase(Locale.getDefault()));
    }

    private String name;

    SerializerType() {
        this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public String getName() {
        return name;
    }
}
