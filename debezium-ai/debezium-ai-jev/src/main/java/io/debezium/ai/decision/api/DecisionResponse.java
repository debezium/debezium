/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Result of an evaluation.
 *
 * @param model identifier of the model which was actually used
 * @param answers answer for every question keyed by the question name
 * @param usage token usage
 *
 * @author Jiri Pechanec
 */
public record DecisionResponse(String model, Map<String, Answer> answers, Usage usage) {

    public DecisionResponse {
        Objects.requireNonNull(model, "model");
        Objects.requireNonNull(answers, "answers");
        Objects.requireNonNull(usage, "usage");
        answers = Collections.unmodifiableMap(new LinkedHashMap<>(answers));
    }
}
