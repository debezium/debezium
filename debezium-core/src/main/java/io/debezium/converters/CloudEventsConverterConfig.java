/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.ConverterConfig;

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

    private static final ConfigDef CONFIG;

    static {
        CONFIG = ConverterConfig.newConfigDef();

        CONFIG.define(CLOUDEVENTS_SERIALIZER_TYPE_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_SERIALIZER_TYPE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_SERIALIZER_TYPE_DOC);
        CONFIG.define(CLOUDEVENTS_DATA_SERIALIZER_TYPE_CONFIG, ConfigDef.Type.STRING, CLOUDEVENTS_DATA_SERIALIZER_TYPE_DEFAULT, ConfigDef.Importance.HIGH,
                CLOUDEVENTS_DATA_SERIALIZER_TYPE_DOC);
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
}
