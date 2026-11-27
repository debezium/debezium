/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

import java.util.List;
import java.util.Objects;

/**
 * Question whose answer is a (possibly fractional) score on a zero based ordered scale.
 *
 * @param name unique name of the question
 * @param instructions text of the question
 * @param levels descriptions of the levels, index in the list is the level number; at least two levels
 *
 * @author Jiri Pechanec
 */
public record ScoreQuestion(String name, String instructions, List<String> levels) implements Question {

    public static final int MIN_LEVELS = 2;

    public ScoreQuestion {
        Checks.requireName(name);
        Checks.requireText(instructions, "instructions of question '" + name + "'");
        Objects.requireNonNull(levels, "levels");
        if (levels.size() < MIN_LEVELS) {
            throw new IllegalArgumentException("Question '" + name + "' must have at least " + MIN_LEVELS + " levels");
        }
        for (var i = 0; i < levels.size(); i++) {
            Checks.requireText(levels.get(i), "description of level " + i + " of question '" + name + "'");
        }
        levels = List.copyOf(levels);
    }
}
