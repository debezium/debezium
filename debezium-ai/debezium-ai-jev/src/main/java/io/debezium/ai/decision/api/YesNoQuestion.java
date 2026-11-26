/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

/**
 * Question whose answer is the probability of a positive reply.
 *
 * @param name unique name of the question
 * @param instructions text of the question
 * @param yesDescription optional description of the positive case; may be {@code null}
 * @param noDescription optional description of the negative case; may be {@code null}
 *
 * @author Jiri Pechanec
 */
public record YesNoQuestion(String name, String instructions, String yesDescription, String noDescription) implements Question {

    public YesNoQuestion {
        Checks.requireName(name);
        Checks.requireText(instructions, "instructions of question '" + name + "'");
    }
}
