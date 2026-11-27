/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.SequencedMap;
import java.util.regex.Pattern;

/**
 * Question whose answer is exactly one of the named options.
 *
 * @param name unique name of the question
 * @param instructions text of the question
 * @param options option name to option description, in the order given; the description may be empty
 *
 * @author Jiri Pechanec
 */
public record ChoiceQuestion(String name, String instructions, SequencedMap<String, String> options) implements Question {

    /**
     * Allowed form of an option name.
     */
    public static final Pattern OPTION_NAME_PATTERN = Pattern.compile("[A-Za-z0-9_-]+");

    public ChoiceQuestion {
        Checks.requireName(name);
        Checks.requireText(instructions, "instructions of question '" + name + "'");
        Objects.requireNonNull(options, "options");
        if (options.isEmpty()) {
            throw new IllegalArgumentException("Question '" + name + "' must have at least one option");
        }
        final SequencedMap<String, String> copy = new LinkedHashMap<>();
        for (final var option : options.entrySet()) {
            if (!isValidOptionName(option.getKey())) {
                throw new IllegalArgumentException("Option '" + option.getKey() + "' of question '" + name + "' must match " + OPTION_NAME_PATTERN.pattern());
            }
            copy.put(option.getKey(), Objects.requireNonNull(option.getValue(), "description of option " + option.getKey()));
        }
        options = Collections.unmodifiableSequencedMap(copy);
    }

    /**
     * @return {@code true} if the given value is a valid option name
     */
    public static boolean isValidOptionName(final String optionName) {
        return optionName != null && OPTION_NAME_PATTERN.matcher(optionName).matches();
    }
}
