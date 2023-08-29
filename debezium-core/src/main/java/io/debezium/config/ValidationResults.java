/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.connector.Connector;

public class ValidationResults {

    public final List<ValidationResult> validationResults;
    public Status status;

    public ValidationResults(Connector connector, Map<String, ?> properties) {
        this.validationResults = convertConfigToValidationResults(connector.validate(convertPropertiesToStrings(properties)));
        if (validationResults.isEmpty()) {
            this.status = Status.VALID;
        }
        else {
            this.status = Status.INVALID;
        }
    }

    private static Map<String, String> convertPropertiesToStrings(Map<String, ?> properties) {
        return properties.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue())));
    }

    private List<ValidationResult> convertConfigToValidationResults(Config result) {
        return result.configValues()
                .stream()
                .filter(cv -> !cv.errorMessages().isEmpty())
                .filter(cv -> !cv.errorMessages().get(0).equals(cv.name() + " is referred in the dependents, but not defined."))
                .map(cv -> new ValidationResult(cv.name(), cv.errorMessages().get(0)))
                .collect(Collectors.toList());
    }

    public static class ValidationResult {
        public String property;
        public String message;

        public ValidationResult(String property, String message) {
            this.property = property;
            this.message = message;
        }
    }

    public enum Status {
        VALID,
        INVALID;
    }

}
