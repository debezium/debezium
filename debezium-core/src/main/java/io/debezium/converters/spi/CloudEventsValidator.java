/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

/**
 * A class validating that a record contains a CloudEvent
 *
 * @author Roman Kudryashov
 */
public class CloudEventsValidator {

    private static final String CLOUD_EVENTS_SCHEMA_NAME_SUFFIX = ".CloudEvents.Envelope";

    private final Set<String> cloudEventsSpecRequiredFields = Set.of(CloudEventsMaker.FieldName.ID, CloudEventsMaker.FieldName.SOURCE,
            CloudEventsMaker.FieldName.SPECVERSION,
            CloudEventsMaker.FieldName.TYPE);

    private SerializerType serializerType;

    public void configure(SerializerType serializerType) {
        this.serializerType = serializerType;
    }

    public boolean isCloudEvent(SchemaAndValue schemaAndValue) {
        return baseCheck(schemaAndValue) && checkFields(schemaAndValue.value());
    }

    public void verifyIsCloudEvent(SchemaAndValue schemaAndValue) {
        if (!isCloudEvent(schemaAndValue)) {
            throw new DataException("A deserialized record's value is not a CloudEvent: value=" + schemaAndValue.value());
        }
    }

    private boolean baseCheck(SchemaAndValue schemaAndValue) {
        switch (serializerType) {
            case JSON:
                return schemaAndValue.schema() == null && schemaAndValue.value() instanceof Map;
            case AVRO:
                return schemaAndValue.schema().name().endsWith(CLOUD_EVENTS_SCHEMA_NAME_SUFFIX) && schemaAndValue.value() instanceof Struct;
            default:
                throw new DataException("Can't check whether a record is a CloudEvent for serializer type \"" + serializerType + "\"");
        }
    }

    private boolean checkFields(Object value) {
        final List<String> fieldNames;
        switch (serializerType) {
            case JSON:
                Map<String, Object> valueMap = (Map<String, Object>) value;
                fieldNames = new ArrayList<>(valueMap.keySet());
                break;
            case AVRO:
                fieldNames = ((Struct) value).schema().fields().stream().map(Field::name).collect(Collectors.toList());
                break;
            default:
                throw new DataException("Can't check whether a record is a CloudEvent for serializer type \"" + serializerType + "\"");
        }

        return fieldNames.size() >= 4 && fieldNames.containsAll(cloudEventsSpecRequiredFields);
    }
}
