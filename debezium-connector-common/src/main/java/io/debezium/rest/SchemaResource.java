/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import java.io.File;
import java.io.IOException;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;

public interface SchemaResource {

    String getSchemaFilePath();

    String SCHEMA_ENDPOINT = "/schema";

    ObjectMapper MAPPER = new ObjectMapper();

    @GET
    @Path(SCHEMA_ENDPOINT)
    default JsonNode getConnectorSchema() {

        try {
            return MAPPER.readValue(getClass().getResourceAsStream(getSchemaFilePath()), JsonNode.class);
        }
        catch (IOException e) {
            throw new DebeziumException("Unable to open \"" + getSchemaFilePath().substring(getSchemaFilePath().lastIndexOf(File.separator) + 1) + "\" schema file.", e);
        }
    }
}
