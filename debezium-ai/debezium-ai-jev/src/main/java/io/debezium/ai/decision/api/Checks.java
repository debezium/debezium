/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

/**
 * Validation helpers shared by the records of this package.
 */
final class Checks {

    private Checks() {
    }

    static void requireName(final String name) {
        if (!Question.isValidName(name)) {
            throw new IllegalArgumentException("Question name '" + name + "' must match " + Question.NAME_PATTERN.pattern());
        }
    }

    static void requireText(final String value, final String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("The " + what + " must not be blank");
        }
    }

    static void requireProbability(final double value, final String what) {
        // negated comparison to reject NaN as well
        if (!(value >= 0.0 && value <= 1.0)) {
            throw new IllegalArgumentException("The " + what + " must be within [0, 1] but was " + value);
        }
    }
}
