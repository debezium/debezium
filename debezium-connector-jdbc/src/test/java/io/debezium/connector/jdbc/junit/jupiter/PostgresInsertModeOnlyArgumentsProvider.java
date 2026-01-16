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

import io.debezium.connector.jdbc.junit.jupiter.PostgresInsertModeArgumentsProvider.PostgresInsertMode;

/**
 * A simplified JUnit {@link ArgumentsProvider} that provides only PostgreSQL insert modes
 * (standard INSERT vs UNNEST optimization) without the SinkRecordFactory variation.
 *
 * Use this when you want to test both INSERT modes with a single SinkRecordFactory,
 * or when you're manually creating records in your test.
 *
 * For combined testing with both SinkRecordFactory variants and insert modes,
 * use {@link PostgresInsertModeArgumentsProvider} instead.
 *
 * @author Gaurav Miglani
 */
public class PostgresInsertModeOnlyArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        return Stream.of(
                Arguments.of(Named.of("Standard INSERT", new PostgresInsertMode("Standard INSERT", false))),
                Arguments.of(Named.of("UNNEST Optimization", new PostgresInsertMode("UNNEST Optimization", true))));
    }
}
