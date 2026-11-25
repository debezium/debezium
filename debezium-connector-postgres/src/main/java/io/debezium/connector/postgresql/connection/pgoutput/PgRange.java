/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.util.function.Function;

/**
 * Represents a Postgres range type like tstzrange or int4range.
 * @param lower lower bound
 * @param upper upper bound
 * @param empty true of the range is empty `(,)`
 * @param lowerInclusive is lower bound inclusive
 * @param upperInclusive is upper bound inclusive
 * @param lowerInfinite is lower bound is open
 * @param upperInfinite is upper bound is open
 * @param valueStringifier a function to convert bound value to its string representation.
 * @param <T> bound value type e.g. timestamp or int
 */
public record PgRange<T>(T lower, T upper, boolean empty, boolean lowerInclusive, boolean upperInclusive, boolean lowerInfinite,
        boolean upperInfinite, Function<T, String> valueStringifier) {

    /**
     * Convert the range into a string representation that's understood by Postgres SQL queries.
     * @return SQL string representation of the range.
     */
    @Override
    public String toString() {
        if (this.empty) {
            return "(,)";
        }

        final var sb = new StringBuilder();

        if (this.lowerInclusive) {
            sb.append("[");
        }
        else {
            sb.append("(");
        }

        if (!this.lowerInfinite) {
            sb.append(this.valueStringifier.apply(this.lower));
        }

        sb.append(",");

        if (!this.upperInfinite) {
            sb.append(this.valueStringifier.apply(this.upper));
        }

        if (this.upperInclusive) {
            sb.append("]");
        }
        else {
            sb.append(")");
        }

        return sb.toString();
    }
}
