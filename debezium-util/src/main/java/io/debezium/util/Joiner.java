/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Iterator;
import java.util.Objects;
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
        return new Joiner(delimiter, "", "");
    }

    public static Joiner on(CharSequence prefix, CharSequence delimiter) {
        return new Joiner(delimiter, prefix, "");
    }

    public static Joiner on(CharSequence prefix, CharSequence delimiter, CharSequence suffix) {
        return new Joiner(delimiter, prefix, suffix);
    }

    private final CharSequence delimiter;
    private final CharSequence prefix;
    private final CharSequence suffix;

    private Joiner(CharSequence delimiter, CharSequence prefix, CharSequence suffix) {
        this.delimiter = Objects.requireNonNull(delimiter);
        this.prefix = Objects.requireNonNull(prefix);
        this.suffix = Objects.requireNonNull(suffix);
    }

    private StringJoiner createStringJoiner() {
        return new StringJoiner(delimiter, prefix, suffix);
    }

    public String join(Object[] values) {
        final var joiner = createStringJoiner();
        if (values != null) {
            for (final var value : values) {
                if (value != null) {
                    joiner.add(value.toString());
                }
            }
        }
        return joiner.toString();
    }

    public String join(CharSequence firstValue, CharSequence... additionalValues) {
        final var joiner = createStringJoiner();
        if (firstValue != null) {
            joiner.add(firstValue);
        }
        if (additionalValues != null) {
            for (final var value : additionalValues) {
                if (value != null) {
                    joiner.add(value);
                }
            }
        }
        return joiner.toString();
    }

    public String join(Iterable<?> values) {
        final var joiner = createStringJoiner();
        if (values != null) {
            for (final var value : values) {
                if (value != null) {
                    joiner.add(value.toString());
                }
            }
        }
        return joiner.toString();
    }

    public String join(Iterable<?> values, CharSequence nextValue, CharSequence... additionalValues) {
        final var joiner = createStringJoiner();
        if (values != null) {
            for (final var value : values) {
                if (value != null) {
                    joiner.add(value.toString());
                }
            }
        }
        if (nextValue != null) {
            joiner.add(nextValue);
        }
        if (additionalValues != null) {
            for (final var value : additionalValues) {
                if (value != null) {
                    joiner.add(value);
                }
            }
        }
        return joiner.toString();
    }

    public String join(Iterator<?> values) {
        final var joiner = createStringJoiner();
        if (values != null) {
            while (values.hasNext()) {
                final var value = values.next();
                if (value != null) {
                    joiner.add(value.toString());
                }
            }
        }
        return joiner.toString();
    }

}
