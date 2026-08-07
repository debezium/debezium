/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.time.Conversions;
import io.debezium.time.Date;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.transforms.neo4j.CudEvent.Operation;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.NodeMode;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.OutputMode;

public class CudEventFactory {

    private final Neo4jCudConverterConfig config;

    public CudEventFactory(Neo4jCudConverterConfig config) {
        this.config = config;
    }

    public List<CudEvent> buildEvents(Struct data, Operation cudOp) {
        if (config.nodeMode() == NodeMode.NODE) {
            return buildNodeModeEvents(data, cudOp);
        }
        return buildRelationshipModeEvents(data, cudOp);
    }

    private List<CudEvent> buildNodeModeEvents(Struct data, Operation cudOp) {
        final List<CudEvent> events = new ArrayList<>();
        events.add(buildNodeEvent(data, cudOp));

        if (cudOp != Operation.DELETE) {
            for (final var mapping : config.relationships()) {
                final var fkValue = columnValue(data, mapping.fkColumn());
                if (fkValue == null) {
                    continue;
                }
                events.add(buildRelationshipForNodeMode(data, cudOp, mapping, fkValue));
            }
        }
        return events;
    }

    private CudNodeEvent buildNodeEvent(Struct data, Operation cudOp) {
        final var ids = extractIdProperties(data);

        if (cudOp == Operation.DELETE) {
            return new CudNodeEvent(cudOp, config.nodeLabels(), ids, null, config.deleteDetach());
        }

        final var excludedColumns = columnsExcludedFromProperties();
        final var properties = extractProperties(data, excludedColumns);
        return new CudNodeEvent(cudOp, config.nodeLabels(), ids, properties, null);
    }

    private CudRelationshipEvent buildRelationshipForNodeMode(Struct data, Operation cudOp,
                                                              RelationshipMapping mapping, Object fkValue) {
        final var sourceIds = extractIdProperties(data);
        final var targetIds = Map.<String, Object> of(mapping.targetId(), fkValue);

        final var from = new CudRelationshipEvent.Endpoint(config.nodeLabels(), sourceIds, Operation.MERGE);
        final var to = new CudRelationshipEvent.Endpoint(
                List.of(mapping.targetLabel()), targetIds, mapping.targetNodeOp());

        final var relProps = extractRelationshipProperties(data, mapping);

        if (mapping.direction() == RelationshipMapping.Direction.INCOMING) {
            return new CudRelationshipEvent(cudOp, mapping.type(), to, from, relProps);
        }
        return new CudRelationshipEvent(cudOp, mapping.type(), from, to, relProps);
    }

    private List<CudEvent> buildRelationshipModeEvents(Struct data, Operation cudOp) {
        final var mappings = config.relationships();
        final var fromMapping = endpointFor(mappings, RelationshipMapping.Direction.OUTGOING);
        final var toMapping = endpointFor(mappings, RelationshipMapping.Direction.INCOMING);

        final var fromFkValue = columnValue(data, fromMapping.fkColumn());
        final var toFkValue = columnValue(data, toMapping.fkColumn());

        final List<CudEvent> events = new ArrayList<>();
        final var isArray = config.outputMode() == OutputMode.ARRAY;

        if (isArray && cudOp != Operation.DELETE) {
            events.add(mergeNodeForEndpoint(fromMapping, fromFkValue));
            events.add(mergeNodeForEndpoint(toMapping, toFkValue));
        }

        final var endpointOp = isArray ? Operation.MATCH : Operation.MERGE;
        events.add(buildJoinRelationship(data, cudOp, fromMapping, toMapping, fromFkValue, toFkValue, endpointOp));
        return events;
    }

    private static RelationshipMapping endpointFor(List<RelationshipMapping> mappings, RelationshipMapping.Direction direction) {
        return mappings.stream()
                .filter(mapping -> mapping.direction() == direction)
                .findFirst()
                // Guaranteed present: config validation requires exactly one outgoing and one incoming endpoint.
                .orElseThrow(() -> new IllegalStateException("No relationship mapping with direction " + direction));
    }

    private CudNodeEvent mergeNodeForEndpoint(RelationshipMapping mapping, Object fkValue) {
        final var ids = Map.<String, Object> of(mapping.targetId(), fkValue);
        return new CudNodeEvent(Operation.MERGE, List.of(mapping.targetLabel()), ids, Collections.emptyMap(), null);
    }

    private CudRelationshipEvent buildJoinRelationship(Struct data, Operation cudOp,
                                                       RelationshipMapping fromMapping, RelationshipMapping toMapping,
                                                       Object fromFkValue, Object toFkValue, Operation endpointOp) {
        final var fromIds = Map.<String, Object> of(fromMapping.targetId(), fromFkValue);
        final var toIds = Map.<String, Object> of(toMapping.targetId(), toFkValue);

        final var from = new CudRelationshipEvent.Endpoint(List.of(fromMapping.targetLabel()), fromIds, endpointOp);
        final var to = new CudRelationshipEvent.Endpoint(List.of(toMapping.targetLabel()), toIds, endpointOp);

        final var fkColumns = config.fkColumns();
        final var relProps = extractNonFkProperties(data, fkColumns, fromMapping);

        // Endpoint roles are fixed by direction (outgoing = source, incoming = target), so the relationship
        // always runs from -> to; the type and properties come from the outgoing (source) mapping.
        return new CudRelationshipEvent(cudOp, fromMapping.type(), from, to, relProps);
    }

    private Map<String, Object> extractIdProperties(Struct data) {
        final Map<String, Object> ids = new LinkedHashMap<>();
        for (final var idProp : config.nodeIdProperties()) {
            final var value = columnValue(data, idProp);
            if (value != null) {
                ids.put(idProp, value);
            }
        }
        return ids;
    }

    /**
     * Reads a column value from the record, normalizing Debezium date logical types to ISO-8601
     * strings.
     * The schema is required to recognize these types, which is why this normalization happens
     * here rather than in the serializer.
     */
    private static Object columnValue(Struct data, Field field) {
        return normalizeTemporal(data.get(field), field.schema());
    }

    private static Object columnValue(Struct data, String fieldName) {
        final var field = data.schema().field(fieldName);
        return normalizeTemporal(data.get(fieldName), field != null ? field.schema() : null);
    }

    /**
     * Converts a Debezium date type value into an ISO-8601 string, or returns the value
     * unchanged for any other type.
     *
     * @param value the raw column value (epoch integer for the handled temporal types)
     * @param schema the value's schema, used to detect the logical type; may be {@code null}
     * @return the ISO-8601 string, or {@code value} unchanged when it is not a handled temporal type
     */
    protected static Object normalizeTemporal(Object value, Schema schema) {
        if (value == null || schema == null || schema.name() == null) {
            return value;
        }
        return switch (schema.name()) {
            case Date.SCHEMA_NAME -> LocalDate.ofEpochDay(((Number) value).longValue()).toString();
            case Timestamp.SCHEMA_NAME -> IsoTimestamp.toIsoString(((Number) value).longValue(), null);
            case MicroTimestamp.SCHEMA_NAME -> IsoTimestamp.toIsoString(Conversions.toInstantFromMicros(((Number) value).longValue()), null);
            default -> value;
        };
    }

    private Map<String, Object> extractProperties(Struct data, Set<String> excluded) {
        final var include = config.propertiesInclude();
        final var useIncludeFilter = !include.isEmpty();
        final Map<String, Object> properties = new LinkedHashMap<>();

        for (final Field field : data.schema().fields()) {
            final var name = field.name();
            if (excluded.contains(name)) {
                continue;
            }
            if (useIncludeFilter && !include.contains(name)) {
                continue;
            }
            final var value = columnValue(data, field);
            if (value != null) {
                properties.put(name, value);
            }
        }
        return properties;
    }

    private Map<String, Object> extractRelationshipProperties(Struct data, RelationshipMapping mapping) {
        if (mapping.properties().isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, Object> props = new LinkedHashMap<>();
        for (final var propName : mapping.properties()) {
            final var value = columnValue(data, propName);
            if (value != null) {
                props.put(propName, value);
            }
        }
        return props;
    }

    private Map<String, Object> extractNonFkProperties(Struct data, Set<String> fkColumns,
                                                       RelationshipMapping mapping) {
        final var relPropertyNames = mapping.properties();
        final Map<String, Object> props = new LinkedHashMap<>();

        for (final Field field : data.schema().fields()) {
            final var name = field.name();
            if (fkColumns.contains(name)) {
                continue;
            }
            if (!relPropertyNames.isEmpty() && !relPropertyNames.contains(name)) {
                continue;
            }
            final var value = columnValue(data, field);
            if (value != null) {
                props.put(name, value);
            }
        }
        return props;
    }

    private Set<String> columnsExcludedFromProperties() {
        final var idProps = Set.copyOf(config.nodeIdProperties());
        final var fkCols = config.fkColumns();
        final var explicitExclude = config.propertiesExclude();

        final Set<String> excluded = new HashSet<>(idProps);
        excluded.addAll(fkCols);
        excluded.addAll(explicitExclude);
        return excluded;
    }
}
