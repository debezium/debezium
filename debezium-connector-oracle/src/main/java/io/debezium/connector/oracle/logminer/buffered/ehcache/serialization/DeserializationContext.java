/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple context object for tracking the deserialization of a {@link DataInputStream}.
 *
 * @author Chris Cranford
 */
public class DeserializationContext {

    private final List<Object> values = new ArrayList<>();

    /**
     * Adds a deserialized value to the context's value list. The value list is used to locate and
     * construct the target object once the object stream has been consumed. Therefore, values are
     * to be added and thus deserialized in the order they appear in the object's constructor.
     *
     * @param value the deserialized value
     */
    public void addValue(Object value) {
        values.add(value);
    }

    /**
     * Gets all the deserialized values in the context.
     *
     * @return list of deserialized values.
     */
    public List<Object> getValues() {
        return values;
    }
}
