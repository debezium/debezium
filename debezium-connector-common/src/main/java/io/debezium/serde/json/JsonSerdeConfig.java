/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.serde.json;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.data.Envelope;

/**
 * A configuration for {@link JsonSerde} serialize/deserializer.
 *
 * @author Jiri Pechanec
 *
 */
@Incubating
public class JsonSerdeConfig extends AbstractConfig {

    public static final Field FROM_FIELD = Field.create("from.field")
            .withDisplayName("What Envelope field should be deserialized (before/after)")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Enables user to choose which of Envelope provided fields should be deserialized as the payload."
                    + "If not set then the envelope is provided as is.")
            .withValidation(JsonSerdeConfig::isEnvelopeFieldName);

    public static final Field UNKNOWN_PROPERTIES_IGNORED = Field.create("unknown.properties.ignored")
            .withDisplayName("Unknown properties ignored")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls whether unknown properties will be ignored or cause a JsonMappingException when encountered.")
            .withDefault(false);

    private static int isEnvelopeFieldName(Configuration config, Field field, ValidationOutput problems) {
        final String fieldName = config.getString(field);
        if (fieldName == null) {
            return 0;
        }
        if (!(Envelope.FieldName.AFTER.equals(fieldName) || Envelope.FieldName.BEFORE.equals(fieldName))) {
            problems.accept(field, fieldName, "Allowed values are 'before' or 'after'");
            return 1;
        }
        return 0;
    }

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef();
        Field.group(CONFIG, "Source", FROM_FIELD, UNKNOWN_PROPERTIES_IGNORED);
    }

    private String sourceField;
    private boolean unknownPropertiesIgnored;

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public JsonSerdeConfig(Map<String, ?> props) {
        super(CONFIG, props);
        this.sourceField = getString(FROM_FIELD.name());
        this.unknownPropertiesIgnored = getBoolean(UNKNOWN_PROPERTIES_IGNORED.name());
    }

    public String sourceField() {
        return sourceField;
    }

    public boolean asEnvelope() {
        return sourceField == null;
    }

    public boolean isUnknownPropertiesIgnored() {
        return unknownPropertiesIgnored;
    }
}
