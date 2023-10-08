/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.ConverterConfig;

import io.debezium.config.CommonConnectorConfig.SchemaNameAdjustmentMode;
import io.debezium.config.EnumeratedValue;
import io.debezium.converters.spi.SerializerType;

/**
 * Configuration options for {@link CloudEventsConverter CloudEventsConverter} instances.
 */
public class CloudEventsConverterConfig extends ConverterConfig {

    public static final String CLOUDEVENTS_SERIALIZER_TYPE_CONFIG = "serializer.type";
    public static final String CLOUDEVENTS_SERIALIZER_TYPE_DEFAULT = "json";
    private static final String CLOUDEVENTS_SERIALIZER_TYPE_DOC = "Specify a serializer to serialize CloudEvents values";

    public static final String CLOUDEVENTS_DATA_SERIALIZER_TYPE_CONFIG = "data.serializer.type";
    public static final String CLOUDEVENTS_DATA_SERIALIZER_TYPE_DEFAULT = "json";
    private static final String CLOUDEVENTS_DATA_SERIALIZER_TYPE_DOC = "Specify a serializer to serialize the data field of CloudEvents values";

    public static final String CLOUDEVENTS_EXTENSION_ATTRIBUTES_ENABLE_CONFIG = "extension.attributes.enable";
    public static final boolean CLOUDEVENTS_EXTENSION_ATTRIBUTES_ENABLE_DEFAULT = true;
    private static final String CLOUDEVENTS_EXTENSION_ATTRIBUTES_ENABLE_DOC = "Specify whether to include extension attributes to a cloud event";

    public static final String CLOUDEVENTS_SCHEMA_NAME_ADJUSTMENT_MODE_CONFIG = "schema.name.adjustment.mode";
    public static final String CLOUDEVENTS_SCHEMA_NAME_ADJUSTMENT_MODE_DEFAULT = "avro";
    private static final String CLOUDEVENTS_SCHEMA_NAME_ADJUSTMENT_MODE_DOC = "Specify how schema names should be adjusted for compatibility with the message converter used by the connector, including:"
            + "'avro' replaces the characters that cannot be used in the Avro type name with underscore (default)"
            + "'none' does not apply any adjustment";

    public static final String CLOUDEVENTS_ID_SOURCE_CONFIG = "id.source";
    public static final String CLOUDEVENTS_ID_SOURCE_DEFAULT = "generate";
    private static final String CLOUDEVENTS_ID_SOURCE_DOC = "Specify how to get id of CloudEvent";

    public static final String CLOUDEVENTS_TYPE_SOURCE_CONFIG = "type.source";
    public static final String CLOUDEVENTS_TYPE_SOURCE_DEFAULT = "generate";
    private static final String CLOUDEVENTS_TYPE_SOURCE_DOC = "Specify how to get type of CloudEvent";

    public static final String CLOUDEVENTS_METADATA_LOCATION_CONFIG = "metadata.location";
    public static final String CLOUDEVENTS_METADATA_LOCATION_DEFAULT = "value";
    private static final String CLOUDEVENTS_METADATA_LOCATION_DOC = "Specify from where to retrieve metadata";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = ConverterConfig.newConfigDef();

        CONFIG.define(CLOUDEVENTS_SERIALIZER_TYPE_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_SERIALIZER_TYPE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_SERIALIZER_TYPE_DOC);
        CONFIG.define(CLOUDEVENTS_DATA_SERIALIZER_TYPE_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_DATA_SERIALIZER_TYPE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_DATA_SERIALIZER_TYPE_DOC);
        CONFIG.define(CLOUDEVENTS_EXTENSION_ATTRIBUTES_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, CLOUDEVENTS_EXTENSION_ATTRIBUTES_ENABLE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_EXTENSION_ATTRIBUTES_ENABLE_DOC);
        CONFIG.define(CLOUDEVENTS_SCHEMA_NAME_ADJUSTMENT_MODE_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_SCHEMA_NAME_ADJUSTMENT_MODE_DEFAULT, ConfigDef.Importance.LOW,
                CLOUDEVENTS_SCHEMA_NAME_ADJUSTMENT_MODE_DOC);
        CONFIG.define(CLOUDEVENTS_ID_SOURCE_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_ID_SOURCE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_ID_SOURCE_DOC);
        CONFIG.define(CLOUDEVENTS_TYPE_SOURCE_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_TYPE_SOURCE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_TYPE_SOURCE_DOC);
        CONFIG.define(CLOUDEVENTS_METADATA_LOCATION_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_METADATA_LOCATION_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_METADATA_LOCATION_DOC);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public CloudEventsConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    /**
     * Return which serializer type is used to serialize CloudEvents values.
     *
     * @return serializer type
     */
    public SerializerType cloudeventsSerializerType() {
        return SerializerType.withName(getString(CLOUDEVENTS_SERIALIZER_TYPE_CONFIG));
    }

    /**
     * Return which serializer type is used to serialize the data field of CloudEvents values.
     *
     * @return serializer type
     */
    public SerializerType cloudeventsDataSerializerTypeConfig() {
        return SerializerType.withName(getString(CLOUDEVENTS_DATA_SERIALIZER_TYPE_CONFIG));
    }

    /**
     * Return whether to include extension attributes in a cloud event.
     *
     * @return whether to enable extension attributes
     */
    public boolean extensionAttributesEnable() {
        return getBoolean(CLOUDEVENTS_EXTENSION_ATTRIBUTES_ENABLE_CONFIG);
    }

    /**
     * Return which adjustment mode is used to build message schema names.
     *
     * @return schema name adjustment mode
     */
    public SchemaNameAdjustmentMode schemaNameAdjustmentMode() {
        return SchemaNameAdjustmentMode.parse(getString(CLOUDEVENTS_SCHEMA_NAME_ADJUSTMENT_MODE_CONFIG));
    }

    /**
     * Return from where to retrieve id of a CloudEvent
     *
     * @return source of id field of a CloudEvent
     */
    public IdSource idSource() {
        return IdSource.parse(getString(CLOUDEVENTS_ID_SOURCE_CONFIG));
    }

    /**
     * Return from where to retrieve type of a CloudEvent
     *
     * @return source of type field of a CloudEvent
     */
    public TypeSource typeSource() {
        return TypeSource.parse(getString(CLOUDEVENTS_TYPE_SOURCE_CONFIG));
    }

    /**
     * Return from where to retrieve metadata
     *
     * @return metadata location
     */
    public MetadataLocation metadataLocation() {
        return MetadataLocation.parse(getString(CLOUDEVENTS_METADATA_LOCATION_CONFIG));
    }

    /**
     * The set of predefined IdSource options
     */
    public enum IdSource implements EnumeratedValue {

        /**
         * Generate id of CloudEvent
         */
        GENERATE("generate"),

        /**
         * Get type of CloudEvent from the header
         */
        HEADER("header");

        private final String value;

        IdSource(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied values is one of the predefined options
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static IdSource parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (IdSource option : IdSource.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    /**
     * The set of predefined TypeSource options
     */
    public enum TypeSource implements EnumeratedValue {

        /**
         * Generate type of CloudEvent
         */
        GENERATE("generate"),

        /**
         * Get type of CloudEvent from the header
         */
        HEADER("header");

        private final String value;

        TypeSource(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied values is one of the predefined options
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static TypeSource parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (TypeSource option : TypeSource.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    /**
     * The set of predefined MetadataLocation options
     */
    public enum MetadataLocation implements EnumeratedValue {

        /**
         * Get metadata from the value
         */
        VALUE("value"),

        /**
         * Get metadata from the header
         */
        HEADER("header");

        private final String value;

        MetadataLocation(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied values is one of the predefined options
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static MetadataLocation parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (MetadataLocation option : MetadataLocation.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }
}
