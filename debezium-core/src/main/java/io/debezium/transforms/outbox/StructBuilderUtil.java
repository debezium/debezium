/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * JSON payload Struct build util util for Debezium Outbox Transform Event Router.
 *
 * @author Laurent Broudoux (laurent.broudoux@gmail.com)
 */
public class StructBuilderUtil {

    /**
    * Convert a Jackson JsonNode into a new Struct according the schema.
    * @param document The JSON document to convert
    * @param schema The Schema for this document
    * @return A new connect Struct for the JSON node.
    */
    public static Struct jsonNodeToStruct(JsonNode document, Schema schema) {
        if (document == null) {
            return null;
        }
        return jsonNodeToStructInternal(document, schema);
    }

    private static Struct jsonNodeToStructInternal(JsonNode document, Schema schema) {
        final Struct struct = new Struct(schema);
        for (Field field : schema.fields()) {
            if (document.has(field.name())) {
                struct.put(field.name(),
                        getStructFieldValue(document.path(field.name()), field.schema()));
            }
        }
        return struct;
    }

    private static Object getStructFieldValue(JsonNode node, Schema schema) {
        switch (node.getNodeType()) {
            case STRING:
                return node.asText();
            case BOOLEAN:
                return node.asBoolean();
            case NUMBER:
                if (node.isFloat()) {
                    return node.floatValue();
                }
                if (node.isDouble()) {
                    return node.asDouble();
                }
                if (node.isInt()) {
                    return node.asInt();
                }
                if (node.isLong()) {
                    return node.asLong();
                }
                return node.decimalValue();
            case ARRAY:
                return getArrayAsList((ArrayNode) node, schema);
            case OBJECT:
                return jsonNodeToStructInternal(node, schema);
            default:
                return null;
        }
    }

    private static List getArrayAsList(ArrayNode array, Schema schema) {
        List arrayObjects = new ArrayList(array.size());
        Iterator<JsonNode> elements = array.elements();
        while (elements.hasNext()) {
            arrayObjects.add(getStructFieldValue(elements.next(), schema.valueSchema()));
        }
        return arrayObjects;
    }
}
