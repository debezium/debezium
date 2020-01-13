/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import java.io.IOException;

import javax.persistence.AttributeConverter;

import org.hibernate.HibernateException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Converter that knows how to convert between a {@link String} and {@link JsonNode}.
 *
 * @author Chris Cranford
 */
public class JsonNodeAttributeConverter implements AttributeConverter<JsonNode, String> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(JsonNode jsonNode) {
        if (jsonNode == null) {
            return null;
        }
        try {
            return mapper.writeValueAsString(jsonNode);
        }
        catch (JsonProcessingException e) {
            throw new HibernateException("Failed to convert JsonNode to String", e);
        }
    }

    @Override
    public JsonNode convertToEntityAttribute(String databaseValue) {
        if (databaseValue == null) {
            return null;
        }
        try {
            return mapper.readValue(databaseValue, JsonNode.class);
        }
        catch (IOException e) {
            throw new HibernateException("Failed to convert String to JsonNode", e);
        }
    }
}
