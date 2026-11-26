/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

/**
 * Answer to a {@link YesNoQuestion}. There is no confidence by design.
 *
 * @param question name of the question
 * @param probability probability of the positive reply within [0, 1]
 *
 * @author Jiri Pechanec
 */
public record YesNoAnswer(String question, double probability) implements Answer {

    public YesNoAnswer {
        Checks.requireText(question, "question name");
        Checks.requireProbability(probability, "probability of question '" + question + "'");
    }
}
