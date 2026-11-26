/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

/**
 * Answer to a {@link Question}.
 *
 * @author Jiri Pechanec
 */
public sealed interface Answer permits ChoiceAnswer, ScoreAnswer, YesNoAnswer {

    /**
     * @return name of the answered question
     */
    String question();
}
