/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Answer to a {@link ScoreQuestion}.
 *
 * @param question name of the question
 * @param score expected score, may be fractional
 * @param confidence confidence within [0, 1]
 * @param probabilities probability of every level within [0, 1], ordered by level
 *
 * @author Jiri Pechanec
 */
public record ScoreAnswer(String question, double score, double confidence, Map<Integer, Double> probabilities) implements Answer {

    public ScoreAnswer {
        Checks.requireText(question, "question name");
        if (!Double.isFinite(score)) {
            throw new IllegalArgumentException("The score of question '" + question + "' must be finite but was " + score);
        }
        Checks.requireProbability(confidence, "confidence of question '" + question + "'");
        Objects.requireNonNull(probabilities, "probabilities");
        final Map<Integer, Double> copy = new TreeMap<>();
        for (final var probability : probabilities.entrySet()) {
            Checks.requireProbability(probability.getValue(), "probability of level " + probability.getKey() + " of question '" + question + "'");
            copy.put(probability.getKey(), probability.getValue());
        }
        probabilities = Collections.unmodifiableMap(copy);
    }
}
