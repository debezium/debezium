/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.schema.SchemaChangeEvent;

/**
 * This SMT to filter schema change event
 * @param <R>
 */
public class SchemaChangeEventFilter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaChangeEventFilter.class);

    private static final Field SCHEMA_CHANGE_EVENT_INCLUDE_LIST = Field.create("schema.change.event.include.list")
            .withDisplayName("Schema change event include list")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription(
                    "Support filtering during DDL synchronization")
            .required();

    private Set<SchemaChangeEvent.SchemaChangeEventType> includeSchemaChangeEvents;
    private SmtManager<R> smtManager;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(SCHEMA_CHANGE_EVENT_INCLUDE_LIST));
        final String includeSchemaChangeEvents = config.getString(SCHEMA_CHANGE_EVENT_INCLUDE_LIST);
        this.includeSchemaChangeEvents = Arrays.stream(includeSchemaChangeEvents.split(",")).map(typeName -> SchemaChangeEvent.SchemaChangeEventType.valueOf(typeName))
                .collect(Collectors.toSet());

    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !smtManager.isValidSchemaChange(record)) {
            return record;
        }
        Struct recordValue = requireStruct(record.value(), "Read schema change event to filter");

        List<Struct> tableChanges = recordValue.getArray(HistoryRecord.Fields.TABLE_CHANGES);
        if (tableChanges == null) {
            LOGGER.debug("Table changes is empty, excluded it.");
            return null;
        }
        SchemaChangeEvent.SchemaChangeEventType schemaChangeEventType;
        if (tableChanges.isEmpty()) {
            schemaChangeEventType = SchemaChangeEvent.SchemaChangeEventType.DATABASE;
        }
        else {
            schemaChangeEventType = SchemaChangeEvent.SchemaChangeEventType.valueOf((String) tableChanges.get(0).get(ConnectTableChangeSerializer.TYPE_KEY));
        }

        if (includeSchemaChangeEvents.contains(schemaChangeEventType)) {
            return record;
        }
        return null;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, SCHEMA_CHANGE_EVENT_INCLUDE_LIST);
        return config;
    }

    @Override
    public void close() {
    }

}
