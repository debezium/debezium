/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.model.debezium;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration group.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Group(
        @JsonProperty("name") String name,
        @JsonProperty("order") Integer order,
        @JsonProperty("description") String description) {
}
