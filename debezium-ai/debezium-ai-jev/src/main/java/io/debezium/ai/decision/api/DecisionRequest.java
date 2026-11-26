/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A single evaluation request: one state and all the questions which are answered over it.
 *
 * @param state JSON-like tree: {@code String}, {@code Number}, {@code Boolean}, {@code Map<String, Object>} or {@code List<Object>}
 * @param questions non-empty list of questions with unique names
 *
 * @author Jiri Pechanec
 */
public record DecisionRequest(Object state, List<Question> questions) {

    public DecisionRequest {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(questions, "questions");
        if (questions.isEmpty()) {
            throw new IllegalArgumentException("At least one question is required");
        }
        final Set<String> names = new HashSet<>();
        for (final var question : questions) {
            if (!names.add(question.name())) {
                throw new IllegalArgumentException("Duplicate question name '" + question.name() + "'");
            }
        }
        questions = List.copyOf(questions);
    }
}
