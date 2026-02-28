/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.data.SchemaUtil;

class EventFormatter {

    private Map<String, String> sourcePosition;
    private Object key;
    private Struct value;
    private final StringBuilder string = new StringBuilder();

    private void printStruct(Struct value) {
        string.append(SchemaUtil.asDetailedString(value));
    }

    private StringBuilder addDelimiter() {
        return string.append(", ");
    }

    private void printSimpleValue(Object key, Object value) {
        string
                .append(key)
                .append(": ")
                .append(value);
    }

    private void removeDelimiter() {
        if (string.length() >= 2 && string.charAt(string.length() - 2) == ',' && string.charAt(string.length() - 1) == ' ') {
            string.delete(string.length() - 2, string.length());
        }
    }

    @Override
    public String toString() {
        if (value != null) {
            final String operation = value.getString(Envelope.FieldName.OPERATION);
            if (operation != null) {
                printSimpleValue("operation", operation);
                addDelimiter();
            }
        }
        if (sourcePosition != null) {
            string.append("position: {");
            sourcePosition.forEach((k, v) -> {
                printSimpleValue(k, v);
                addDelimiter();
            });
            removeDelimiter();
            string.append("}");
            addDelimiter();
        }
        if (key != null) {
            if (key instanceof Struct) {
                string
                        .append("key: ");
                printStruct((Struct) key);
            }
            else {
                printSimpleValue("key", key);
            }
            addDelimiter();
        }
        if (value != null) {
            final Struct before = value.getStruct(Envelope.FieldName.BEFORE);
            final Struct after = value.getStruct(Envelope.FieldName.AFTER);
            if (before != null) {
                string
                        .append("before: ");
                printStruct(before);
                addDelimiter();
            }
            if (after != null) {
                string
                        .append("after: ");
                printStruct(after);
                addDelimiter();
            }
        }
        removeDelimiter();
        return string.toString();
    }

    public EventFormatter sourcePosition(Map<String, String> sourcePosition) {
        this.sourcePosition = sourcePosition;
        return this;
    }

    public EventFormatter key(Object key) {
        this.key = key;
        return this;
    }

    public EventFormatter value(Struct value) {
        this.value = value;
        return this;
    }
}
