/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.actions.AbstractSnapshotSignal.DataCollections;

/**
 * Verifies that the {@code data-collections} parsing shared by the snapshot signals rejects malformed
 * (non-string) entries instead of throwing a {@link NullPointerException} that stops the connector.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/798">DBZ-6352</a>
 */
class SnapshotSignalDataCollectionsTest {

    private final DocumentReader reader = DocumentReader.defaultReader();

    private Document parse(String json) throws IOException {
        return reader.read(json);
    }

    @Test
    void executeSnapshotShouldParseStringDataCollections() throws IOException {
        DataCollections result = ExecuteSnapshot.getDataCollections(parse("{\"data-collections\": [\"schema.table1\", \"schema.table2\"]}"));

        assertThat(result.valid()).isTrue();
        assertThat(result.collections()).containsExactly("schema.table1", "schema.table2");
    }

    @Test
    void executeSnapshotShouldBeInvalidForNestedArrayDataCollections() throws IOException {
        LogInterceptor logInterceptor = new LogInterceptor(ExecuteSnapshot.class);

        DataCollections result = ExecuteSnapshot.getDataCollections(parse("{\"data-collections\": [[\"schema.table1\", \"schema.table2\"]]}"));

        assertThat(result.valid()).isFalse();
        assertThat(logInterceptor.containsWarnMessage("contains a non-string data collection")).isTrue();
    }

    @Test
    void executeSnapshotShouldBeInvalidWhenAnyDataCollectionIsNotAString() throws IOException {
        DataCollections result = ExecuteSnapshot.getDataCollections(parse("{\"data-collections\": [\"schema.table1\", 42]}"));

        assertThat(result.valid()).isFalse();
    }

    @Test
    void executeSnapshotShouldBeInvalidWhenDataCollectionsMissingOrEmpty() throws IOException {
        assertThat(ExecuteSnapshot.getDataCollections(parse("{}")).valid()).isFalse();
        assertThat(ExecuteSnapshot.getDataCollections(parse("{\"data-collections\": []}")).valid()).isFalse();
    }

    @Test
    void stopSnapshotShouldParseStringDataCollections() throws IOException {
        DataCollections result = StopSnapshot.getDataCollections(parse("{\"data-collections\": [\"schema.table1\"]}"));

        assertThat(result.valid()).isTrue();
        assertThat(result.collections()).containsExactly("schema.table1");
    }

    @Test
    void stopSnapshotShouldBeInvalidForNestedArrayDataCollections() throws IOException {
        LogInterceptor logInterceptor = new LogInterceptor(StopSnapshot.class);

        DataCollections result = StopSnapshot.getDataCollections(parse("{\"data-collections\": [[\"schema.table1\"]]}"));

        assertThat(result.valid()).isFalse();
        assertThat(logInterceptor.containsWarnMessage("contains a non-string data collection")).isTrue();
    }

    @Test
    void stopSnapshotShouldStopAllWhenDataCollectionsMissingOrEmpty() throws IOException {
        // A missing or empty data-collections is a valid request to stop the entire snapshot, distinct from a
        // malformed one; both must stay separable so a malformed signal is skipped rather than escalating to stop-all.
        DataCollections missing = StopSnapshot.getDataCollections(parse("{}"));
        assertThat(missing.valid()).isTrue();
        assertThat(missing.collections()).isNull();

        DataCollections empty = StopSnapshot.getDataCollections(parse("{\"data-collections\": []}"));
        assertThat(empty.valid()).isTrue();
        assertThat(empty.collections()).isNull();
    }
}
