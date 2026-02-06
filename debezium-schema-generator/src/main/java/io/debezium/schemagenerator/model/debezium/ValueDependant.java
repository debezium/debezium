/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.model.debezium;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Value-based field dependency.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ValueDependant(
        @JsonProperty("values") List<String> values,
        @JsonProperty("dependants") List<String> dependants) {
}
