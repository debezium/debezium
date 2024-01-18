/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Strings;

/**
 * This SMT to extract the changed and unchanged field names to Connect Headers comparing before and after value.
 * It only works on update event.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Harvey Yue
 */
public class ExtractChangedRecordState<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final Field HEADER_CHANGED_NAME = Field.create("header.changed.name")
            .withDisplayName("Header change name.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Specify the header changed name, default is null which means not send changes to header.");

    public static final Field HEADER_UNCHANGED_NAME = Field.create("header.unchanged.name")
            .withDisplayName("Header unchanged name.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Specify the header unchanged name of schema, default is null which means not send changes to header.");

    private String headerChangedName = null;
    private String headerUnchangedName = null;
    private Schema changedSchema;
    private Schema unchangedSchema;
    private SmtManager<R> smtManager;

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);

        if (config.getString(HEADER_CHANGED_NAME) != null) {
            headerChangedName = config.getString(HEADER_CHANGED_NAME);
            changedSchema = SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().name(headerChangedName).build();
        }
        if (config.getString(HEADER_UNCHANGED_NAME) != null) {
            headerUnchangedName = config.getString(HEADER_UNCHANGED_NAME);
            unchangedSchema = SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().name(headerUnchangedName).build();
        }
    }

    @Override
    public R apply(final R record) {
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }

        Struct value = requireStruct(record.value(), "Record value should be struct.");
        Object after = value.get("after");
        Object before = value.get("before");
        if (after != null && before != null) {
            List<String> changedNames = new ArrayList<>();
            List<String> unchangedNames = new ArrayList<>();
            Struct afterValue = requireStruct(after, "After value should be struct.");
            Struct beforeValue = requireStruct(before, "Before value should be struct.");
            afterValue.schema().fields().forEach(field -> {
                if (!Objects.equals(afterValue.get(field), beforeValue.get(field))) {
                    changedNames.add(field.name());
                }
                else {
                    unchangedNames.add(field.name());
                }
            });
            if (!Strings.isNullOrBlank(headerChangedName)) {
                record.headers().add(headerChangedName, changedNames, changedSchema);
            }
            if (!Strings.isNullOrBlank(headerUnchangedName)) {
                record.headers().add(headerUnchangedName, unchangedNames, unchangedSchema);
            }
        }

        return record;
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, HEADER_CHANGED_NAME, HEADER_UNCHANGED_NAME);
        return config;
    }
}
