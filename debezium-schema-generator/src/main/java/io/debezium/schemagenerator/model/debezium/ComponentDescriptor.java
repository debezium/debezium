/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.model.debezium;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Root model for Debezium Descriptor format (DDD-13).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name", "type", "version", "metadata", "properties", "groups" })
public record ComponentDescriptor(
        @JsonProperty("name") String name,
        @JsonProperty("type") String type,
        @JsonProperty("version") String version,
        @JsonProperty("metadata") Metadata metadata,
        @JsonProperty("properties") List<Property> properties,
        @JsonProperty("groups") List<Group> groups) {
}
