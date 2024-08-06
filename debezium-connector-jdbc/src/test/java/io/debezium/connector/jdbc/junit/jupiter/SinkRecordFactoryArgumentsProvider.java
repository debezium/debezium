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
 * A Junit {@link ArgumentsProvider} that provides implementations of the {@link SinkRecordFactory}
 * that are to be used during the test invocation.
 *
 * @author Chris Cranford
 */
public class SinkRecordFactoryArgumentsProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        return Stream.of(
                Arguments.of(Named.of(DebeziumSinkRecordFactory.class.getSimpleName(), new DebeziumSinkRecordFactory())),
                Arguments.of(Named.of(FlatSinkRecordFactory.class.getSimpleName(), new FlatSinkRecordFactory())));
    }
}
