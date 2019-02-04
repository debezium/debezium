/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;

class EventFormatter {

    private final String INDENT = "";

    private Map<String, String> sourcePosition;
    private Object key;
    private Struct value;
    private final StringBuilder string = new StringBuilder();

    private void printStruct(Struct value, String indent) {
        string.append("{");
        value.schema().fields().forEach(field -> {
            final Object fieldValue = value.get(field);
            if (fieldValue != null) {
                if (fieldValue instanceof Struct) {
                    string
                        .append(indent)
                        .append(field.name());
                    printStruct((Struct)fieldValue, indent + INDENT);
                }
                else {
                    printSimpleValue(field.name(), fieldValue, indent);
                }
                addComma();
            }
        });
        removeComma();
        string.append("}");
    }

    private StringBuilder addComma() {
        return string.append(',');
    }

    private void printSimpleValue(Object key, Object value, String indent) {
        string
            .append(INDENT)
            .append(key)
            .append(": ")
            .append(value);
    }

    private void printSimpleValue(Object key, Object value) {
        printSimpleValue(key, value, "");
    }

    private void removeComma() {
        if (string.charAt(string.length() - 1) == ',') {
            string.deleteCharAt(string.length() - 1);
        }
    }
    @Override
    public String toString() {
        if (value != null) {
            final String operation = value.getString(Envelope.FieldName.OPERATION);
            if (operation != null) {
                printSimpleValue("operation", operation);
                addComma();
            }
        }
        if (sourcePosition != null) {
            string.append("position: {");
            sourcePosition.forEach((k, v) -> {
                printSimpleValue(k, v, INDENT);
                addComma();
            });
            removeComma();
            string.append("}");
            addComma();
        }
        if (key != null) {
            if (key instanceof Struct) {
                string
                    .append("key: ");
                printStruct((Struct)key, INDENT);
            }
            else {
                printSimpleValue("key", key);
            }
            addComma();
        }
        if (value != null) {
            final Struct before = value.getStruct(Envelope.FieldName.BEFORE);
            final Struct after = value.getStruct(Envelope.FieldName.AFTER);
            if (before != null) {
                string
                    .append("before: ");
                printStruct(before, INDENT);
                addComma();
            }
            if (after != null) {
                string
                    .append("after: ");
                printStruct(after, INDENT);
                addComma();
            }
        }
        removeComma();
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

