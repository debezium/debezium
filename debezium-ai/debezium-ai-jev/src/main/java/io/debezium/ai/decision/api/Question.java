/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

import java.util.regex.Pattern;

/**
 * A question which is evaluated by a {@link DecisionService} over the state of a record.
 * The set of question kinds is closed, every {@link DecisionService} implementation has to support all of them
 * or reject the unsupported ones when it is created.
 *
 * @author Jiri Pechanec
 */
public sealed interface Question permits ChoiceQuestion, ScoreQuestion, YesNoQuestion {

    /**
     * Allowed form of {@link #name()}.
     */
    Pattern NAME_PATTERN = Pattern.compile("[A-Za-z][A-Za-z0-9_-]{0,63}");

    /**
     * @return unique name of the question within a {@link DecisionRequest}
     */
    String name();

    /**
     * @return non-blank text of the question
     */
    String instructions();

    /**
     * @return {@code true} if the given value is a valid question name
     */
    static boolean isValidName(final String name) {
        return name != null && NAME_PATTERN.matcher(name).matches();
    }
}
