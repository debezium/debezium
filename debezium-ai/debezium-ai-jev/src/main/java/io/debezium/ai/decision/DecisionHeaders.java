/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import io.debezium.connector.common.DebeziumHeaders;

/**
 * Names of the record headers written by {@link DecisionToHeaders}.
 * Nothing else is ever written under the {@link #PREFIX}.
 *
 * @author Jiri Pechanec
 */
public final class DecisionHeaders {

    public static final String PREFIX = DebeziumHeaders.DEBEZIUM_HEADER_PREFIX + "ai.decision.";

    /** Identifier of the model which was actually used. */
    public static final String MODEL = PREFIX + "model";
    /** {@link #STATUS_SUCCESS} or {@link #STATUS_ERROR}. */
    public static final String STATUS = PREFIX + "status";
    /** Sanitized error message, written only on failure in the {@code header} error mode. */
    public static final String ERROR = PREFIX + "error";

    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_ERROR = "ERROR";

    public static final String CONFIDENCE_SUFFIX = ".confidence";
    public static final String PROBABILITIES_SUFFIX = ".probabilities";

    private DecisionHeaders() {
    }

    /** @return name of the header carrying the main result of the question */
    public static String answer(final String question) {
        return PREFIX + question;
    }

    /** @return name of the header carrying the confidence of the answer to the question */
    public static String confidence(final String question) {
        return PREFIX + question + CONFIDENCE_SUFFIX;
    }

    /** @return name of the header carrying JSON object with the probabilities of the answer to the question */
    public static String probabilities(final String question) {
        return PREFIX + question + PROBABILITIES_SUFFIX;
    }
}
