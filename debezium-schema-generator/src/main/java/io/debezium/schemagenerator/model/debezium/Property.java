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
 * Configuration property.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name", "type", "required", "display", "validation", "valueDependants" })
public record Property(
        @JsonProperty("name") String name,
        @JsonProperty("type") String type,
        @JsonProperty("required") Boolean required,
        @JsonProperty("display") Display display,
        @JsonProperty("validation") List<Validation> validation,
        @JsonProperty("valueDependants") List<ValueDependant> valueDependants) {
}
