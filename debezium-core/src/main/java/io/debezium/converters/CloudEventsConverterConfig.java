/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.storage.ConverterConfig;

import io.debezium.config.CommonConnectorConfig.SchemaNameAdjustmentMode;
import io.debezium.config.EnumeratedValue;
import io.debezium.converters.spi.CloudEventsMaker;
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

    @Deprecated
    public static final String CLOUDEVENTS_METADATA_LOCATION_CONFIG = "metadata.location";

    public static final String CLOUDEVENTS_METADATA_SOURCE_CONFIG = "metadata.source";
    public static final String CLOUDEVENTS_METADATA_SOURCE_DEFAULT = "value,id:generate,type:generate";
    private static final String CLOUDEVENTS_METADATA_SOURCE_DOC = "Specify from where to retrieve metadata";

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
        CONFIG.define(CLOUDEVENTS_METADATA_SOURCE_CONFIG, ConfigDef.Type.LIST, CLOUDEVENTS_METADATA_SOURCE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_METADATA_SOURCE_DOC);
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
     * Return from where to retrieve metadata
     *
     * @return metadata source
     */
    public MetadataSource metadataSource() {
        List<String> metadataSources = getList(CLOUDEVENTS_METADATA_SOURCE_CONFIG);

        // get global metadata source
        Set<MetadataSourceValue> globalMetadataSourceAllowedValues = Set.of(MetadataSourceValue.VALUE, MetadataSourceValue.HEADER);
        MetadataSourceValue global = MetadataSourceValue.parse(metadataSources.get(0));
        if (!globalMetadataSourceAllowedValues.contains(global)) {
            throw new ConfigException("Global metadata source can't be " + global.name());
        }

        // get sources for customizable fields
        MetadataSourceValue idCustomSource = null;
        MetadataSourceValue typeCustomSource = null;
        for (int i = 1; i < metadataSources.size(); i++) {
            final String[] parts = metadataSources.get(i).split(":");
            final String fieldName = parts[0];
            final MetadataSourceValue fieldSource = MetadataSourceValue.parse(parts[1]);
            if (fieldSource == null) {
                throw new ConfigException("Field source `" + parts[1] + "` is not valid");
            }
            switch (fieldName) {
                case CloudEventsMaker.FieldName.ID:
                    idCustomSource = fieldSource;
                    break;
                case CloudEventsMaker.FieldName.TYPE:
                    typeCustomSource = fieldSource;
                    break;
                default:
                    throw new ConfigException("Field `" + fieldName + "` is not allowed to set custom source");
            }
        }

        return new MetadataSource(global, idCustomSource != null ? idCustomSource : global, typeCustomSource != null ? typeCustomSource : global);
    }

    public class MetadataSource {
        private final MetadataSourceValue global;
        private final MetadataSourceValue id;
        private final MetadataSourceValue type;

        public MetadataSource(MetadataSourceValue global, MetadataSourceValue id, MetadataSourceValue type) {
            this.global = global;
            this.id = id;
            this.type = type;
        }

        public MetadataSourceValue global() {
            return global;
        }

        public MetadataSourceValue id() {
            return id;
        }

        public MetadataSourceValue type() {
            return type;
        }
    }

    /**
     * The set of predefined MetadataSourceValue options
     */
    public enum MetadataSourceValue implements EnumeratedValue {

        /**
         * Get metadata from the value
         */
        VALUE("value"),

        /**
         * Get metadata from the header
         */
        HEADER("header"),

        /**
         * Generate a field's value
         */
        GENERATE("generate");

        private final String value;

        MetadataSourceValue(String value) {
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
        public static MetadataSourceValue parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (MetadataSourceValue option : MetadataSourceValue.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }
}
