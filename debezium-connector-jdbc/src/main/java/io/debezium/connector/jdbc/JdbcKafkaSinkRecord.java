/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.annotation.Immutable;
import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;

/**
 * An immutable representation of a {@link SinkRecord}.
 *
 * @author Chris Cranford
 * @author rk3rn3r
 */
@Immutable
public class JdbcKafkaSinkRecord extends KafkaDebeziumSinkRecord implements JdbcSinkRecord {

    private final Map<String, JdbcFieldDescriptor> jdbcFields = new LinkedHashMap<>();
    private final PrimaryKeyMode primaryKeyMode;
    private final Set<String> configuredPrimaryKeyFields;
    private final FieldNameFilter fieldFilter;
    private final DatabaseDialect dialect;
    private Struct filteredKey = null;

    // LAZY INIT
    private Set<String> keyFieldNames = null;
    private Set<String> nonKeyFieldNames = null;

    public JdbcKafkaSinkRecord(SinkRecord record, PrimaryKeyMode primaryKeyMode, Set<String> configuredPrimaryKeyFields, FieldNameFilter fieldFilter,
                               String cloudEventsSchemaNamePattern, DatabaseDialect dialect) {
        super(record, cloudEventsSchemaNamePattern);
        this.primaryKeyMode = primaryKeyMode;
        this.configuredPrimaryKeyFields = configuredPrimaryKeyFields;
        this.fieldFilter = fieldFilter;
        this.dialect = dialect;
        if (PrimaryKeyMode.KAFKA.equals(primaryKeyMode)) {
            allFields.forEach((name, field) -> jdbcFields.put(name, new JdbcFieldDescriptor(field, dialect.getSchemaType(field.getSchema()), true)));
            keyFieldNames = new LinkedHashSet<>(allFields().keySet());
        }
    }

    public Struct filteredKey() {
        if (null == filteredKey) {
            filteredKey = getFilteredKey(primaryKeyMode, configuredPrimaryKeyFields, fieldFilter);
        }
        return filteredKey;
    }

    @Override
    public Set<String> keyFieldNames() {
        if (null == keyFieldNames) {
            Struct filteredKey = filteredKey();
            if (null == filteredKey) {
                keyFieldNames = Set.of();
            }
            else {
                keyFieldNames = filteredKey.schema().fields().stream().map(field -> {
                    String fieldName = field.name();
                    FieldDescriptor descriptor = new FieldDescriptor(field.schema(), fieldName, true);
                    allFields.put(fieldName, descriptor);
                    jdbcFields.put(fieldName, new JdbcFieldDescriptor(descriptor, dialect.getSchemaType(field.schema()), true));
                    return fieldName;
                }).collect(Collectors.toCollection(LinkedHashSet::new));
            }
        }
        return keyFieldNames;
    }

    @Override
    public Set<String> nonKeyFieldNames() {
        if (null == nonKeyFieldNames) {
            final Struct filteredPayload = getFilteredPayload(fieldFilter);
            if (null == filteredPayload) {
                nonKeyFieldNames = Set.of();
            }
            else {
                nonKeyFieldNames = filteredPayload.schema().fields().stream().map(field -> {
                    String fieldName = field.name();
                    if (allFields.containsKey(fieldName) || keyFieldNames.contains(fieldName)) {
                        return null;
                    }
                    FieldDescriptor descriptor = new FieldDescriptor(field.schema(), fieldName, false);
                    allFields.put(fieldName, descriptor);
                    jdbcFields.put(fieldName, new JdbcFieldDescriptor(descriptor, dialect.getSchemaType(field.schema()), false));
                    return fieldName;
                }).filter(Objects::nonNull).collect(Collectors.toCollection(LinkedHashSet::new));
            }
        }
        return nonKeyFieldNames;
    }

    @Override
    public Map<String, JdbcFieldDescriptor> jdbcFields() {
        if (!isTruncate() && !isTombstone() && jdbcFields.isEmpty()) {
            if (null == keyFieldNames) {
                keyFieldNames();
            }
            if (null == nonKeyFieldNames) {
                nonKeyFieldNames();
            }
        }
        return jdbcFields;
    }

    @Override
    public String toString() {
        return "JdbcKafkaSinkRecord{" +
                "jdbcFields=" + jdbcFields +
                ", primaryKeyMode=" + primaryKeyMode +
                ", configuredPrimaryKeyFields=" + configuredPrimaryKeyFields +
                ", keyFieldNames=" + keyFieldNames() +
                ", nonKeyFieldNames=" + nonKeyFieldNames() +
                '}';
    }
}
