/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.model.debezium;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Property display information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Display(
        @JsonProperty("label") String label,
        @JsonProperty("description") String description,
        @JsonProperty("group") String group,
        @JsonProperty("groupOrder") Integer groupOrder,
        @JsonProperty("width") String width,
        @JsonProperty("importance") String importance) {
}
