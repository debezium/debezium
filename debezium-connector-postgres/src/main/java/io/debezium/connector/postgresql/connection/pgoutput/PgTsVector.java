/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A record to represent Postgres tsvector
 *
 * @param lexemes
 */
public record PgTsVector(List<Lexeme> lexemes) {

    @Override
    public String toString() {
        return this.lexemes.stream()
                .map(Lexeme::toString)
                .collect(Collectors.joining(" "));
    }

    public enum Weight {
        D,
        C,
        B,
        A;

        public static Weight valueOf(final int ordinal) {
            if (ordinal < 0 || ordinal >= Weight.values().length) {
                throw new IllegalArgumentException("Invalid ordinal: " + ordinal);
            }
            return values()[ordinal];
        }
    }

    public record Lexeme(String lexeme, List<Position> positions) {

        @Override
        public String toString() {
            return "'%s':%s".formatted(this.lexeme.replace("'", "''").replace("\\", "\\\\"),
                    this.positions.stream()
                            .map(Position::toString)
                            .collect(Collectors.joining(",")));
        }
    }

    public record Position(int position, Weight weight) {

        @Override
        public String toString() {
            return "%d%s".formatted(this.position, this.weight == Weight.D ? "" : this.weight.toString());
        }
    }
}
