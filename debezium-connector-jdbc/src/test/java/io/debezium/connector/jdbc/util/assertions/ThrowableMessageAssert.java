/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util.assertions;

import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.internal.Failures;

/**
 * Debezium custom assertion for matching exception messages up the exception hierarchy.
 *
 * @author Chris Cranford
 */
public class ThrowableMessageAssert<T extends Throwable> extends AbstractThrowableAssert<ThrowableMessageAssert<T>, T> {

    public ThrowableMessageAssert(T actual) {
        super(actual, ThrowableMessageAssert.class);
    }

    public ThrowableMessageAssert<T> hasMessageMatchingRegEx(String regex) {
        isNotNull();

        Throwable current = actual;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().matches(regex)) {
                return this;
            }
            current = current.getCause();
        }

        throw Failures.instance().failure(String.format("Expected any message in the exception or its causes to match regex: <%s>, but none did.", regex));
    }

    public ThrowableMessageAssert<T> hasMessageContainingText(String description) {
        isNotNull();

        Throwable current = actual;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(description)) {
                return this;
            }
            current = current.getCause();
        }

        throw Failures.instance().failure(String.format("Expected any message in the exception or its causes to contain: <%s>, but none did.", description));
    }

    public static <T extends Throwable> ThrowableMessageAssert<T> assertThatThrowable(T actual) {
        return new ThrowableMessageAssert<>(actual);
    }
}
