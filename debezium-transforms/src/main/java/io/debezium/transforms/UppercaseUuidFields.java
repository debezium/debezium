/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * SMT that uppercases the values of specified UUID columns.
 *
 * <p>Configuration example:
 * <pre>
 * "transforms": "uuidUpper",
 * "transforms.uuidUpper.type": "io.debezium.transforms.UppercaseUuidFields",
 * "transforms.uuidUpper.columns": "dbo.Table1.Column1,dbo.Table2.Column2"
 * </pre>
 *
 * <p>Column format: {@code schema.table.column} (e.g. {@code dbo.MyTable.MyUuidCol}).
 */
public class UppercaseUuidFields<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(UppercaseUuidFields.class);

    public static final String COLUMNS_CONF = "columns";

    public static final Field COLUMNS_FIELD = Field.create(COLUMNS_CONF)
            .withDisplayName("UUID columns to uppercase")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription(
                    "Comma-separated list of columns whose values should be uppercased. "
                            + "Format: schema.table.column (e.g. dbo.MyTable.MyUuidCol)")
            .required();

    // Key: "schema.table" -> Set of column names
    private final Map<String, Set<String>> columnsByTable = new HashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        Configuration config = Configuration.from(configs);
        List<String> columns = config.getStrings(COLUMNS_FIELD, ",");
        if (columns == null) {
            return;
        }
        for (String col : columns) {
            String trimmed = col.trim();
            int lastDot = trimmed.lastIndexOf('.');
            if (lastDot <= 0) {
                LOGGER.warn("Invalid column format '{}', expected schema.table.column", trimmed);
                continue;
            }
            String tableKey = trimmed.substring(0, lastDot).toLowerCase();
            String columnName = trimmed.substring(lastDot + 1).toLowerCase();
            columnsByTable.computeIfAbsent(tableKey, k -> new HashSet<>()).add(columnName);
        }
        LOGGER.info("UppercaseUuidFields configured for: {}", columnsByTable);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct)) {
            return record;
        }

        Struct envelope = (Struct) record.value();

        // Extract schema and table from the source struct
        Struct source = safeGetStruct(envelope, "source");
        if (source == null) {
            return record;
        }

        String schemaName = safeGetString(source, "schema");
        String tableName = safeGetString(source, "table");
        if (schemaName == null || tableName == null) {
            return record;
        }

        String tableKey = (schemaName + "." + tableName).toLowerCase();
        Set<String> columnsToUppercase = columnsByTable.get(tableKey);
        if (columnsToUppercase == null) {
            return record;
        }

        uppercaseFields(safeGetStruct(envelope, "before"), columnsToUppercase);
        uppercaseFields(safeGetStruct(envelope, "after"), columnsToUppercase);

        return record;
    }

    private void uppercaseFields(Struct struct, Set<String> columns) {
        if (struct == null) {
            return;
        }
        for (org.apache.kafka.connect.data.Field field : struct.schema().fields()) {
            if (columns.contains(field.name().toLowerCase())) {
                String value = struct.getString(field.name());
                if (value != null) {
                    struct.put(field.name(), value.toUpperCase());
                }
            }
        }
    }

    private static Struct safeGetStruct(Struct struct, String fieldName) {
        if (struct.schema().field(fieldName) != null) {
            return struct.getStruct(fieldName);
        }
        return null;
    }

    private static String safeGetString(Struct struct, String fieldName) {
        if (struct.schema().field(fieldName) != null) {
            return struct.getString(fieldName);
        }
        return null;
    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        Field.group(config, null, COLUMNS_FIELD);
        return config;
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }
}
