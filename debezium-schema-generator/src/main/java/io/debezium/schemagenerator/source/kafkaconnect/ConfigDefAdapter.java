/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

/**
 * Adapts Kafka Connect {@link ConfigDef} to Debezium {@link Field.Set}.
 *
 * <p>This adapter bridges the gap between Kafka Connect's configuration definition format
 * and Debezium's Field-based configuration schema. It converts each ConfigDef.ConfigKey
 * into a corresponding Debezium Field with appropriate mappings for type, importance,
 * width, and other metadata.
 *
 * <p><b>Mapping details:</b>
 * <ul>
 *   <li><b>Type</b> - Direct mapping from KC Type to Debezium Type</li>
 *   <li><b>Importance</b> - Direct mapping from KC Importance to Debezium Importance</li>
 *   <li><b>Width</b> - Direct mapping from KC Width to Debezium Width</li>
 *   <li><b>Group</b> - KC string groups are not mapped (Debezium uses Group enum)</li>
 *   <li><b>Validators</b> - Not directly mappable (KC validators differ from Debezium)</li>
 * </ul>
 *
 * <p><b>Example usage:</b>
 * <pre>{@code
 * ConfigDef kcConfigDef = ...; // from KC component
 * ConfigDefAdapter adapter = new ConfigDefAdapter();
 * Field.Set fields = adapter.adapt(kcConfigDef);
 * }</pre>
 */
public class ConfigDefAdapter {

    private static final Logger LOGGER = System.getLogger(ConfigDefAdapter.class.getName());

    /**
     * Adapts a Kafka Connect ConfigDef to a Debezium Field.Set.
     *
     * <p>Each ConfigKey in the ConfigDef is converted to a Field with appropriate
     * metadata mappings. Fields that cannot be converted are logged and skipped.
     *
     * @param configDef the Kafka Connect configuration definition
     * @return a Field.Set containing converted fields, never null (may be empty)
     */
    public Field.Set adapt(ConfigDef configDef) {

        List<Field> fields = new ArrayList<>();

        for (ConfigDef.ConfigKey configKey : configDef.configKeys().values()) {
            try {
                Field field = convertConfigKeyToField(configKey);
                fields.add(field);
            }
            catch (Exception e) {
                LOGGER.log(Level.WARNING,
                        "Could not convert ConfigKey " + configKey.name + " to Field", e);
            }
        }

        LOGGER.log(Level.DEBUG,
                "Converted " + fields.size() + " ConfigKeys to Fields");

        return Field.setOf(fields);
    }

    /**
     * Converts a single ConfigDef.ConfigKey to a Debezium Field.
     *
     * @param configKey the KC config key to convert
     * @return the converted Field
     */
    private Field convertConfigKeyToField(ConfigDef.ConfigKey configKey) {

        // Start with basic field creation
        Field field = Field.create(
                configKey.name,
                configKey.displayName != null ? configKey.displayName : configKey.name,
                configKey.documentation);

        // Set type
        field = field.withType(configKey.type);

        // Set importance
        field = field.withImportance(configKey.importance);

        // Set width if present
        if (configKey.width != null) {
            field = field.withWidth(configKey.width);
        }

        // Set default value if present (ConfigDef uses NO_DEFAULT_VALUE constant for absent defaults)
        if (configKey.defaultValue != null &&
                configKey.defaultValue != ConfigDef.NO_DEFAULT_VALUE) {
            field = setDefaultValue(field, configKey);
        }

        return field;
    }

    /**
     * Sets the default value on a Field based on the ConfigKey's type.
     *
     * <p>This method handles type-specific default value setting since
     * Debezium Field has typed withDefault() methods.
     *
     * @param field the field to set default on
     * @param configKey the config key containing the default value
     * @return the field with default value set
     */
    private Field setDefaultValue(Field field, ConfigDef.ConfigKey configKey) {

        Object defaultValue = configKey.defaultValue;

        return switch (configKey.type) {
            case BOOLEAN -> {
                if (defaultValue instanceof Boolean) {
                    yield field.withDefault((Boolean) defaultValue);
                }
                yield field.withDefault(defaultValue.toString());
            }
            case INT -> {
                if (defaultValue instanceof Number) {
                    yield field.withDefault(((Number) defaultValue).intValue());
                }
                yield field.withDefault(defaultValue.toString());
            }
            case LONG -> {
                if (defaultValue instanceof Number) {
                    yield field.withDefault(((Number) defaultValue).longValue());
                }
                yield field.withDefault(defaultValue.toString());
            }
            case SHORT, DOUBLE, STRING, PASSWORD, CLASS, LIST -> field.withDefault(defaultValue.toString());
            default -> {
                LOGGER.log(Level.DEBUG,
                        "Unknown type " + configKey.type + " for field " + configKey.name +
                                ", using string default");
                yield field.withDefault(defaultValue.toString());
            }
        };
    }
}
