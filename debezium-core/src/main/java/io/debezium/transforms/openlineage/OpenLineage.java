/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.openlineage;

import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetType.OUTPUT;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.util.BoundedConcurrentHashMap;

public class OpenLineage<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineage.class);

    private static final int CACHE_SIZE = 64;

    private ZonedDateTime lastEmissionTime;
    private final BoundedConcurrentHashMap<String, Boolean> recentlySeenTopics = new BoundedConcurrentHashMap<>(CACHE_SIZE);
    private final BoundedConcurrentHashMap<Schema, Boolean> recentlySeenSchemas = new BoundedConcurrentHashMap<>(CACHE_SIZE);

    @Override
    public ConfigDef config() {

        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> props) {
    }

    @Override
    public R apply(R record) {

        if (record.value() == null) {
            return record;
        }

        if (recentlySeenTopics.put(record.topic(), true) == null || recentlySeenSchemas.put(record.valueSchema(), true) == null) {

            if (recentlySeenSchemas.put(record.valueSchema(), true) == null) {

                List<DatasetMetadata.FieldDefinition> fieldDefinitions = record.valueSchema().fields().stream()
                        .map(this::buildFieldDefinition)
                        .toList();
                DebeziumOpenLineageEmitter.emit(BaseSourceTask.State.RUNNING, List.of(new DatasetMetadata(record.topic(), OUTPUT, fieldDefinitions)));

                lastEmissionTime = ZonedDateTime.now();
            }

            LOGGER.debug("Emitting running event for output dataset {}", record.topic());

            return record;
        }

        if (Duration.between(lastEmissionTime.toInstant(), ZonedDateTime.now().toInstant()).getSeconds() >= 120) {
            DebeziumOpenLineageEmitter.emit(BaseSourceTask.State.RUNNING);
            LOGGER.debug("Emitting periodic running event. No new output dataset detected");
        }

        return record;
    }

    private DatasetMetadata.FieldDefinition buildFieldDefinition(Field field) {

        Schema schema = field.schema();
        String name = field.name();
        String typeName = schema.type().name();
        String description = schema.doc();

        if (schema.type() == Schema.Type.STRUCT && schema.fields() != null && !schema.fields().isEmpty()) {

            List<DatasetMetadata.FieldDefinition> nestedFields = schema.fields().stream()
                    .map(this::buildFieldDefinition)
                    .toList();

            return new DatasetMetadata.FieldDefinition(name, typeName, description, nestedFields);
        }

        return new DatasetMetadata.FieldDefinition(name, typeName, description);
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }
}
