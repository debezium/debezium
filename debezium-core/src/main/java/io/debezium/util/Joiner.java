/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Iterator;
import java.util.StringJoiner;

import io.debezium.annotation.Immutable;

/**
 * A utility for joining multiple {@link CharSequence character sequences} together. One major difference compared to
 * {@link StringJoiner} is that this class ignores null values (rather than appending "null").
 *
 * @author Randall Hauch
 */
@Immutable
public final class Joiner {

    public static Joiner on(CharSequence delimiter) {
        return new Joiner(new StringJoiner(delimiter));
    }

    public static Joiner on(CharSequence prefix, CharSequence delimiter) {
        return new Joiner(new StringJoiner(delimiter, prefix, ""));
    }

    public static Joiner on(CharSequence prefix, CharSequence delimiter, CharSequence suffix) {
        return new Joiner(new StringJoiner(delimiter, prefix, suffix));
    }

    private final StringJoiner joiner;

    private Joiner(StringJoiner joiner) {
        this.joiner = joiner;
    }

    public String join(Object[] values) {
        for (Object value : values) {
            if (value != null) {
                joiner.add(value.toString());
            }
        }
        return joiner.toString();
    }

    public String join(CharSequence firstValue, CharSequence... additionalValues) {
        if (firstValue != null) {
            joiner.add(firstValue);
        }
        for (CharSequence value : additionalValues) {
            if (value != null) {
                joiner.add(value);
            }
        }
        return joiner.toString();
    }

    public String join(Iterable<?> values) {
        for (Object value : values) {
            if (value != null) {
                joiner.add(value.toString());
            }
        }
        return joiner.toString();
    }

    public String join(Iterable<?> values, CharSequence nextValue, CharSequence... additionalValues) {
        for (Object value : values) {
            if (value != null) {
                joiner.add(value.toString());
            }
        }
        if (nextValue != null) {
            joiner.add(nextValue);
        }
        for (CharSequence value : additionalValues) {
            if (value != null) {
                joiner.add(value);
            }
        }
        return joiner.toString();
    }

    public String join(Iterator<?> values) {
        while (values.hasNext()) {
            Object value = values.next();
            if (value != null) {
                joiner.add(value.toString());
            }
        }
        return joiner.toString();
    }

}
