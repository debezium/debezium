/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.time.Conversions;
import io.debezium.time.Date;
import io.debezium.time.IsoTime;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.transforms.neo4j.CudEvent.Operation;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.NodeMode;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.OutputMode;

public class CudEventFactory {

    private final Neo4jCudConverterConfig config;

    public CudEventFactory(Neo4jCudConverterConfig config) {
        this.config = config;
    }

    public List<CudEvent> buildEvents(Struct data, Operation cudOp, TableMappingConfig mapping) {
        if (mapping.nodeMode() == NodeMode.NODE) {
            return buildNodeModeEvents(data, cudOp, mapping);
        }
        return buildRelationshipModeEvents(data, cudOp, mapping);
    }

    private List<CudEvent> buildNodeModeEvents(Struct data, Operation cudOp, TableMappingConfig mapping) {
        final List<CudEvent> events = new ArrayList<>();
        events.add(buildNodeEvent(data, cudOp, mapping));

        if (cudOp != Operation.DELETE) {
            for (final var relMapping : mapping.relationships()) {
                columnValue(data, relMapping.fkColumn()).ifPresent(fkValue -> events.add(
                        buildRelationshipForNodeMode(data, cudOp, mapping, relMapping, fkValue)));
            }
        }
        return events;
    }

    private CudNodeEvent buildNodeEvent(Struct data, Operation cudOp, TableMappingConfig mapping) {
        final var ids = extractIdProperties(data, mapping);

        if (cudOp == Operation.DELETE) {
            return new CudNodeEvent(cudOp, mapping.nodeLabels(), ids, null, mapping.deleteDetach());
        }

        final var excludedColumns = columnsExcludedFromProperties(mapping);
        final var properties = extractProperties(data, excludedColumns, mapping);
        return new CudNodeEvent(cudOp, mapping.nodeLabels(), ids, properties, null);
    }

    private CudRelationshipEvent buildRelationshipForNodeMode(Struct data, Operation cudOp, TableMappingConfig mapping,
                                                              RelationshipMapping relMapping, Object fkValue) {
        final var sourceIds = extractIdProperties(data, mapping);
        final var targetIds = Map.<String, Object> of(relMapping.targetId(), fkValue);

        final var from = new CudRelationshipEvent.Endpoint(mapping.nodeLabels(), sourceIds, Operation.MERGE);
        final var to = new CudRelationshipEvent.Endpoint(
                List.of(relMapping.targetLabel()), targetIds, relMapping.targetNodeOp());

        final var relProps = cudOp == Operation.DELETE ? null : extractRelationshipProperties(data, relMapping);

        if (relMapping.direction() == RelationshipMapping.Direction.INCOMING) {
            return new CudRelationshipEvent(cudOp, relMapping.type(), to, from, relProps);
        }
        return new CudRelationshipEvent(cudOp, relMapping.type(), from, to, relProps);
    }

    private List<CudEvent> buildRelationshipModeEvents(Struct data, Operation cudOp, TableMappingConfig mapping) {
        final var mappings = mapping.relationships();
        final var fromMapping = endpointFor(mappings, RelationshipMapping.Direction.OUTGOING);
        final var toMapping = endpointFor(mappings, RelationshipMapping.Direction.INCOMING);
        final var fromFkValue = requiredFkValue(data, fromMapping, mapping);
        final var toFkValue = requiredFkValue(data, toMapping, mapping);
        final List<CudEvent> events = new ArrayList<>();
        final var isArray = config.outputMode() == OutputMode.ARRAY;

        if (isArray && cudOp != Operation.DELETE) {
            events.add(mergeNodeForEndpoint(fromMapping, fromFkValue));
            events.add(mergeNodeForEndpoint(toMapping, toFkValue));
        }

        final var endpointOp = isArray ? Operation.MATCH : Operation.MERGE;
        events.add(buildJoinRelationship(data, cudOp, fromMapping, toMapping, fromFkValue, toFkValue, endpointOp, mapping));
        return events;
    }

    private static RelationshipMapping endpointFor(List<RelationshipMapping> mappings, RelationshipMapping.Direction direction) {
        return mappings.stream()
                .filter(mapping -> mapping.direction() == direction)
                .findFirst()
                // Guaranteed present: config validation requires exactly one outgoing and one incoming endpoint.
                .orElseThrow(() -> new IllegalStateException("No relationship mapping with direction " + direction));
    }

    private Object requiredFkValue(Struct data, RelationshipMapping relMapping, TableMappingConfig mapping) {
        return columnValue(data, relMapping.fkColumn())
                .orElseThrow(() -> new DataException(String.format(
                        "Table '%s' is mapped as a relationship, but its foreign key column '%s' is missing "
                                + "from the record or has a null value; cannot build the relationship endpoint",
                        mapping.tableName(), relMapping.fkColumn())));
    }

    private CudNodeEvent mergeNodeForEndpoint(RelationshipMapping mapping, Object fkValue) {
        final var ids = Map.<String, Object> of(mapping.targetId(), fkValue);
        return new CudNodeEvent(Operation.MERGE, List.of(mapping.targetLabel()), ids, Collections.emptyMap(), null);
    }

    private CudRelationshipEvent buildJoinRelationship(Struct data, Operation cudOp,
                                                       RelationshipMapping fromMapping, RelationshipMapping toMapping,
                                                       Object fromFkValue, Object toFkValue, Operation endpointOp,
                                                       TableMappingConfig mapping) {
        final var fromIds = Map.<String, Object> of(fromMapping.targetId(), fromFkValue);
        final var toIds = Map.<String, Object> of(toMapping.targetId(), toFkValue);

        final var from = new CudRelationshipEvent.Endpoint(List.of(fromMapping.targetLabel()), fromIds, endpointOp);
        final var to = new CudRelationshipEvent.Endpoint(List.of(toMapping.targetLabel()), toIds, endpointOp);

        final var fkColumns = mapping.fkColumns();
        final var relProps = cudOp == Operation.DELETE ? null : extractNonFkProperties(data, fkColumns, fromMapping);

        // Endpoint roles are fixed by direction (outgoing = source, incoming = target), so the relationship
        // always runs from -> to; the type and properties come from the outgoing (source) mapping.
        return new CudRelationshipEvent(cudOp, fromMapping.type(), from, to, relProps);
    }

    private Map<String, Object> extractIdProperties(Struct data, TableMappingConfig mapping) {
        final Map<String, Object> ids = new LinkedHashMap<>();
        for (final var idProp : mapping.nodeIdProperties()) {
            columnValue(data, idProp).ifPresent(value -> ids.put(idProp, value));
        }
        return ids;
    }

    /**
     * Reads a column value from the record, normalizing Debezium and Kafka Connect temporal logical
     * types to ISO-8601 strings.
     * The schema is required to recognize these types, which is why this normalization happens
     * here rather than in the serializer.
     */
    private static Optional<Object> columnValue(Struct data, Field field) {
        return Optional.ofNullable(normalizeTemporal(data.get(field), field.schema()));
    }

    private static Optional<Object> columnValue(Struct data, String fieldName) {
        final var field = data.schema().field(fieldName);
        return field == null ? Optional.empty() : columnValue(data, field);
    }

    /**
     * Converts a Debezium or Kafka Connect temporal logical-type value into an ISO-8601 string, or
     * returns the value unchanged for any other type.
     * Coverage spans the temporal types a source connector can emit for a column, independent of the
     * connector's {@code time.precision.mode}:
     *
     * @param value the raw column value (an epoch number for the Debezium types, a {@link java.util.Date}
     *            for the Connect logical types)
     * @param schema the value's schema, used to detect the logical type; may be {@code null}
     * @return the ISO-8601 string, or {@code value} unchanged when it is not a handled temporal type
     */
    protected static Object normalizeTemporal(Object value, Schema schema) {
        if (value == null || schema == null || schema.name() == null) {
            return value;
        }
        return switch (schema.name()) {
            case Date.SCHEMA_NAME -> LocalDate.ofEpochDay(((Number) value).longValue()).toString();
            case Time.SCHEMA_NAME -> IsoTime.toIsoString(Duration.ofMillis(((Number) value).longValue()), false);
            case MicroTime.SCHEMA_NAME -> IsoTime.toIsoString(Duration.of(((Number) value).longValue(), ChronoUnit.MICROS), false);
            case NanoTime.SCHEMA_NAME -> IsoTime.toIsoString(Duration.ofNanos(((Number) value).longValue()), false);
            case Timestamp.SCHEMA_NAME -> IsoTimestamp.toIsoString(((Number) value).longValue(), null);
            case MicroTimestamp.SCHEMA_NAME -> IsoTimestamp.toIsoString(Conversions.toInstantFromMicros(((Number) value).longValue()), null);
            case NanoTimestamp.SCHEMA_NAME -> IsoTimestamp.toIsoString(Conversions.toInstantFromNanos(((Number) value).longValue()), null);
            case org.apache.kafka.connect.data.Date.LOGICAL_NAME -> ((java.util.Date) value).toInstant().atOffset(ZoneOffset.UTC).toLocalDate().toString();
            case org.apache.kafka.connect.data.Time.LOGICAL_NAME ->
                IsoTime.toIsoString(((java.util.Date) value).toInstant().atOffset(ZoneOffset.UTC).toLocalTime(), false);
            case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME -> IsoTimestamp.toIsoString(((java.util.Date) value).toInstant(), null);
            default -> value;
        };
    }

    private Map<String, Object> extractProperties(Struct data, Set<String> excluded, TableMappingConfig mapping) {
        final var include = mapping.propertiesInclude();
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
            columnValue(data, field).ifPresent(value -> properties.put(name, value));
        }
        return properties;
    }

    private Map<String, Object> extractRelationshipProperties(Struct data, RelationshipMapping mapping) {
        if (mapping.properties().isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, Object> props = new LinkedHashMap<>();
        for (final var propName : mapping.properties()) {
            columnValue(data, propName).ifPresent(value -> props.put(propName, value));
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
            columnValue(data, field).ifPresent(value -> props.put(name, value));
        }
        return props;
    }

    private Set<String> columnsExcludedFromProperties(TableMappingConfig mapping) {
        final var idProps = Set.copyOf(mapping.nodeIdProperties());
        final var fkCols = mapping.fkColumns();
        final var explicitExclude = mapping.propertiesExclude();

        final Set<String> excluded = new HashSet<>(idProps);
        excluded.addAll(fkCols);
        excluded.addAll(explicitExclude);
        return excluded;
    }
}
