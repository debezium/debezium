/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client.payloads;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnySetter;

/**
 * Represents a payload event's set of values, i.e. {@code before} or {@code after} values
 * representing the state of columns within the specific payload event.
 *
 * @author Chris Cranford
 */
public class Values {

    private Map<String, Object> values = new HashMap<>();

    public Map<String, Object> getValues() {
        return values;
    }

    @JsonAnySetter
    public void addProperty(String key, Object value) {
        values.put(key, value);
    }

    @Override
    public String toString() {
        return "Values{" +
                "values=" + values +
                '}';
    }

}
