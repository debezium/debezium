/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest.model;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;

public class ValidationResults {

    public Status status;
    public final List<ValidationResult> validationResults;

    public ValidationResults(Config validatedConfig) {
        this.validationResults = convertConfigToValidationResults(validatedConfig);
        if (validationResults.isEmpty()) {
            this.status = Status.VALID;
        }
        else {
            this.status = Status.INVALID;
        }
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
