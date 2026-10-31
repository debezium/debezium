/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.doc.FixFor;

/**
 * Serializes and deserializes each {@link LogMinerRedoEvent} shape directly with the Ehcache serializer, including
 * redo SQL longer than {@code writeUTF} can hold.
 *
 * @author Chris Cranford
 */
public class LogMinerRedoEventSerializerTest {

    private final LogMinerRedoEventSerializer serializer = new LogMinerRedoEventSerializer(getClass().getClassLoader());

    @ParameterizedTest(name = "{0}")
    @MethodSource("events")
    @FixFor("debezium/dbz#2943")
    public void shouldRoundTripEvent(LogMinerRedoEvent event) throws Exception {
        final ByteBuffer buffer = serializer.serialize(event);
        final LogMinerRedoEvent actual = serializer.read(buffer);
        LogMinerRedoEventFixtures.assertSameEvent(event, actual);
    }

    static Stream<Arguments> events() {
        return LogMinerRedoEventFixtures.allEvents().stream().map(event -> Arguments.of(Named.of(event.getEventType().name(), event)));
    }
}
