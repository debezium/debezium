/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.VariableScaleDecimal;

public class CudEventSerializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public String serializeSingle(CudEvent event) {
        return toJson(serializeEvent(event));
    }

    public String serializeArray(List<CudEvent> events) {
        return toJson(events.stream()
                .map(this::serializeEvent)
                .collect(Collectors.toList()));
    }

    private Map<String, Object> serializeEvent(CudEvent event) {
        return event instanceof CudNodeEvent node ? toNodeMap(node) : toRelationshipMap((CudRelationshipEvent) event);
    }

    private Map<String, Object> toNodeMap(CudNodeEvent node) {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", node.type());
        map.put("op", node.op().value());
        map.put("labels", node.labels());
        map.put("ids", toPropertyValues(node.ids()));

        if (node.properties() != null) {
            map.put("properties", toPropertyValues(node.properties()));
        }
        if (node.detach() != null) {
            map.put("detach", node.detach());
        }
        return map;
    }

    private Map<String, Object> toRelationshipMap(CudRelationshipEvent rel) {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", rel.type());
        map.put("op", rel.op().value());
        map.put("rel_type", rel.relType());
        map.put("from", toEndpointMap(rel.from()));
        map.put("to", toEndpointMap(rel.to()));
        map.put("properties", toPropertyValues(rel.properties()));
        return map;
    }

    private Map<String, Object> toEndpointMap(CudRelationshipEvent.Endpoint endpoint) {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("labels", endpoint.labels());
        map.put("ids", toPropertyValues(endpoint.ids()));
        map.put("op", endpoint.op().value());
        return map;
    }

    private Map<String, Object> toPropertyValues(Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return properties;
        }
        final Map<String, Object> converted = new LinkedHashMap<>();
        for (final var entry : properties.entrySet()) {
            final var value = toValue(entry.getValue());
            if (value != null) {
                converted.put(entry.getKey(), value);
            }
        }
        return converted;
    }

    Object toValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Byte b) {
            return b.longValue();
        }
        if (value instanceof Short s) {
            return s.longValue();
        }
        if (value instanceof Integer i) {
            return i.longValue();
        }
        if (value instanceof Float f) {
            return f.doubleValue();
        }
        if (value instanceof BigDecimal bigDecimal) {
            // Neo4j has no arbitrary-precision numeric type.
            // With decimal.handling.mode=precise a decimal reaches the SMT as a BigDecimal; serialize it to its exact string representation
            // rather than risking precision loss by converting to a Double.
            return bigDecimal.toPlainString();
        }
        if (value instanceof Struct struct) {
            if (VariableScaleDecimal.LOGICAL_NAME.equals(struct.schema().name())) {
                return VariableScaleDecimal.toLogical(struct).toString();
            }
            return structToJson(struct);
        }
        if (value instanceof Map<?, ?> m) {
            return toJson(m);
        }
        return value;
    }

    private String structToJson(Struct struct) {
        final Map<String, Object> map = new LinkedHashMap<>();
        for (final Field field : struct.schema().fields()) {
            final var fieldValue = struct.get(field);
            if (fieldValue != null) {
                map.put(field.name(), toValue(fieldValue));
            }
        }
        return toJson(map);
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        }
        catch (JsonProcessingException e) {
            throw new ConnectException("Failed to serialize CUD event to JSON", e);
        }
    }

}
