/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.cloudevents;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.ConverterConfig;

/**
 * Configuration options for {@link CloudEventsConverter CloudEventsConverter} instances.
 */
public class CloudEventsConverterConfig extends ConverterConfig {

    public static final String CLOUDEVENTS_SERIALIZER_TYPE_CONFIG = "cloudevents.serializer.type";
    public static final String CLOUDEVENTS_SERIALIZER_TYPE_DEFAULT = "json";
    private static final String CLOUDEVENTS_SERIALIZER_TYPE_DOC = "Specify a serializer to serialize CloudEvents values";

    public static final String CLOUDEVENTS_DATA_SERIALIZER_TYPE_CONFIG = "cloudevents.data.serializer.type";
    public static final String CLOUDEVENTS_DATA_SERIALIZER_TYPE_DEFAULT = "json";
    private static final String CLOUDEVENTS_DATA_SERIALIZER_TYPE_DOC = "Specify a serializer to serialize the data field of CloudEvents values";

    public static final String CLOUDEVENTS_JSON_SCHEMAS_ENABLE_CONFIG = "json.schemas.enable";
    public static final boolean CLOUDEVENTS_JSON_SCHEMAS_ENABLE_DEFAULT = false;
    private static final String CLOUDEVENTS_JSON_SCHEMAS_ENABLE_DOC = "Include json schemas within each of the serialized CloudEvents data.";

    public static final String CLOUDEVENTS_SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String CLOUDEVENTS_SCHEMA_REGISTRY_URL_DOC = "Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas for CloudEvents.";

    public static final String CLOUDEVENTS_DATA_SCHEMA_REGISTRY_URL_CONFIG = "data.schema.registry.url";
    private static final String CLOUDEVENTS_DATA_SCHEMA_REGISTRY_URL_DOC = "Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas for the data field of CloudEvents.";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = ConverterConfig.newConfigDef();

        CONFIG.define(CLOUDEVENTS_SERIALIZER_TYPE_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_SERIALIZER_TYPE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_SERIALIZER_TYPE_DOC);
        CONFIG.define(CLOUDEVENTS_DATA_SERIALIZER_TYPE_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_DATA_SERIALIZER_TYPE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_DATA_SERIALIZER_TYPE_DOC);
        CONFIG.define(CLOUDEVENTS_JSON_SCHEMAS_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, CLOUDEVENTS_JSON_SCHEMAS_ENABLE_DEFAULT, ConfigDef.Importance.MEDIUM,
                CLOUDEVENTS_JSON_SCHEMAS_ENABLE_DOC);
        CONFIG.define(CLOUDEVENTS_SCHEMA_REGISTRY_URL_CONFIG, ConfigDef.Type.LIST, CLOUDEVENTS_SCHEMA_REGISTRY_URL_DOC, ConfigDef.Importance.MEDIUM,
                CLOUDEVENTS_SCHEMA_REGISTRY_URL_DOC);
        CONFIG.define(CLOUDEVENTS_DATA_SCHEMA_REGISTRY_URL_CONFIG, ConfigDef.Type.LIST, CLOUDEVENTS_DATA_SCHEMA_REGISTRY_URL_DOC, ConfigDef.Importance.MEDIUM,
                CLOUDEVENTS_DATA_SCHEMA_REGISTRY_URL_DOC);
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
     * Return whether schemas of the data field are enabled when using json serializer.
     *
     * @return true if enabled, or false otherwise
     */
    public Boolean cloudeventsJsonSchemasEnable() {
        return getBoolean(CLOUDEVENTS_JSON_SCHEMAS_ENABLE_CONFIG);
    }

    /**
     * Return a list of URLs for schema registry instances that can be used to register or look up schemas for CloudEvents.
     *
     * @return a list of schema registry URLs
     */
    public List<String> cloudeventsSchemaRegistryUrls() {
        return getList(CLOUDEVENTS_SCHEMA_REGISTRY_URL_CONFIG);
    }

    /**
     * Return a list of URLs for schema registry instances that can be used to register or look up schemas for the data field of CloudEvents.
     *
     * @return a list of schema registry URLs
     */
    public List<String> cloudeventsDataSchemaRegistryUrls() {
        return getList(CLOUDEVENTS_DATA_SCHEMA_REGISTRY_URL_CONFIG);
    }
}
