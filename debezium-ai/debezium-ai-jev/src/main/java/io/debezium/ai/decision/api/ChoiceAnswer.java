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
 * Answer to a {@link ChoiceQuestion}.
 *
 * @param question name of the question
 * @param choice the selected option
 * @param confidence confidence of the selection within [0, 1]
 * @param probabilities probability of every option within [0, 1], in the order given
 *
 * @author Jiri Pechanec
 */
public record ChoiceAnswer(String question, String choice, double confidence, Map<String, Double> probabilities) implements Answer {

    public ChoiceAnswer {
        Checks.requireText(question, "question name");
        Checks.requireText(choice, "choice of question '" + question + "'");
        Checks.requireProbability(confidence, "confidence of question '" + question + "'");
        Objects.requireNonNull(probabilities, "probabilities");
        final Map<String, Double> copy = new LinkedHashMap<>();
        for (final var probability : probabilities.entrySet()) {
            Checks.requireProbability(probability.getValue(), "probability of option '" + probability.getKey() + "' of question '" + question + "'");
            copy.put(probability.getKey(), probability.getValue());
        }
        probabilities = Collections.unmodifiableMap(copy);
    }
}
