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
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;
import io.debezium.sink.field.FieldDescriptor;

/**
 * An immutable representation of a {@link SinkRecord} with extensions for the JDBC connector.
 *
 * @author Chris Cranford
 * @author rk3rn3r
 */
@Immutable
public class JdbcKafkaSinkRecord extends KafkaDebeziumSinkRecord implements JdbcSinkRecord {

    private final Map<String, JdbcFieldDescriptor> jdbcFields = new LinkedHashMap<>();
    private final JdbcSinkConnectorConfig config;
    private Struct filteredKey = null;

    // LAZY INIT
    private Set<String> keyFieldNames = null;
    private Set<String> nonKeyFieldNames = null;

    public JdbcKafkaSinkRecord(SinkRecord record, JdbcSinkConnectorConfig config) {
        super(record, config.cloudEventsSchemaNamePattern());
        this.config = config;
        if (PrimaryKeyMode.KAFKA.equals(config.getPrimaryKeyMode())) {
            Map<String, FieldDescriptor> kafkaFields = kafkaFields();
            kafkaFields.forEach((name, field) -> jdbcFields.put(name, new JdbcFieldDescriptor(field, true)));
            allFields.putAll(kafkaFields);
            keyFieldNames = new LinkedHashSet<>(kafkaFields.keySet());
        }
    }

    public Struct filteredKey() {
        if (null == filteredKey) {
            filteredKey = getFilteredKey(config.getPrimaryKeyMode(), config.getPrimaryKeyFields(), config.fieldFilter());
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
                    jdbcFields.put(fieldName, new JdbcFieldDescriptor(descriptor, true));
                    return fieldName;
                }).collect(Collectors.toCollection(LinkedHashSet::new));
            }
        }
        return keyFieldNames;
    }

    @Override
    public Set<String> nonKeyFieldNames() {
        if (null == nonKeyFieldNames) {
            final Struct filteredPayload = getFilteredPayload(config.fieldFilter());
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
                    jdbcFields.put(fieldName, new JdbcFieldDescriptor(descriptor, false));
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
                ", config=" + config +
                ", keyFieldNames=" + keyFieldNames() +
                ", nonKeyFieldNames=" + nonKeyFieldNames() +
                ", originalKafkaRecord=" + originalKafkaRecord +
                '}';
    }
}
