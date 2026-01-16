/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.FlatSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;

/**
 * A JUnit {@link ArgumentsProvider} that provides combinations of {@link SinkRecordFactory}
 * implementations and PostgreSQL insert modes (standard INSERT vs UNNEST optimization).
 * This allows tests to run with all combinations:
 * - DebeziumSinkRecordFactory with standard INSERT
 * - DebeziumSinkRecordFactory with UNNEST
 * - FlatSinkRecordFactory with standard INSERT
 * - FlatSinkRecordFactory with UNNEST
 *
 * @author Gaurav Miglani
 */
public class PostgresInsertModeArgumentsProvider implements ArgumentsProvider {

    /**
     * Represents the PostgreSQL insert mode configuration.
     */
    public static class PostgresInsertMode {
        private final String displayName;
        private final boolean useUnnest;

        public PostgresInsertMode(String displayName, boolean useUnnest) {
            this.displayName = displayName;
            this.useUnnest = useUnnest;
        }

        public String getDisplayName() {
            return displayName;
        }

        public boolean isUnnestEnabled() {
            return useUnnest;
        }

        @Override
        public String toString() {
            return displayName;
        }
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        PostgresInsertMode standardMode = new PostgresInsertMode("Standard INSERT", false);
        PostgresInsertMode unnestMode = new PostgresInsertMode("UNNEST Optimization", true);

        return Stream.of(
                // DebeziumSinkRecordFactory with both modes
                Arguments.of(
                        Named.of(DebeziumSinkRecordFactory.class.getSimpleName() + " - Standard INSERT",
                                new DebeziumSinkRecordFactory()),
                        Named.of("Standard INSERT", standardMode)),
                Arguments.of(
                        Named.of(DebeziumSinkRecordFactory.class.getSimpleName() + " - UNNEST",
                                new DebeziumSinkRecordFactory()),
                        Named.of("UNNEST Optimization", unnestMode)),

                // FlatSinkRecordFactory with both modes
                Arguments.of(
                        Named.of(FlatSinkRecordFactory.class.getSimpleName() + " - Standard INSERT",
                                new FlatSinkRecordFactory()),
                        Named.of("Standard INSERT", standardMode)),
                Arguments.of(
                        Named.of(FlatSinkRecordFactory.class.getSimpleName() + " - UNNEST",
                                new FlatSinkRecordFactory()),
                        Named.of("UNNEST Optimization", unnestMode)));
    }
}
