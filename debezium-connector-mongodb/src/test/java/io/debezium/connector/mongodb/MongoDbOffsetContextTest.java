/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

/**
 * Unit tests for {@link MongoDbOffsetContext} and its {@link MongoDbOffsetContext.Loader}.
 * Specifically tests the fix for loading offsets with {@code initsync=true}.
 */
public class MongoDbOffsetContextTest {

    private static final String REPLICA_SET_NAME = "myReplicaSet";

    private MongoDbConnectorConfig connectorConfig;
    private MongoDbOffsetContext.Loader loader;

    @BeforeEach
    public void beforeEach() {
        connectorConfig = new MongoDbConnectorConfig(Configuration.create()
                .with(MongoDbConnectorConfig.CONNECTION_STRING, "mongodb://localhost:2017/?replicaSet=" + REPLICA_SET_NAME)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .build());

        loader = new MongoDbOffsetContext.Loader(connectorConfig);
    }

    /**
     * Bug 1 fix: When loading an offset with {@code initsync=true}, the Loader must
     * call {@code startInitialSnapshot()} so that {@code isInitialSnapshotRunning()}
     * returns true. Before the fix, {@code isInitialSnapshotRunning()} returned false
     * because {@code isSnapshotRunning()} was never set, causing the connector to
     * skip snapshot recovery and enter a permanent crash loop.
     */
    @Test
    public void loaderShouldRecognizeIncompleteSnapshotFromOffset() {
        // Simulate an offset stored mid-snapshot (initsync=true, no resume token)
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.INITIAL_SYNC, true);
        offset.put(SourceInfo.TIMESTAMP, 0);
        offset.put(SourceInfo.ORDER, 0);

        MongoDbOffsetContext context = loader.load(offset);

        // The critical assertion: isInitialSnapshotRunning() must be true
        assertThat(context.isInitialSnapshotRunning())
                .as("Offset with initsync=true should be recognized as an incomplete snapshot")
                .isTrue();
    }

    /**
     * Verify that a normal offset (no initsync flag) correctly reports that
     * initial snapshot is NOT running.
     */
    @Test
    public void loaderShouldNotFlagSnapshotForNormalOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.TIMESTAMP, 1666193824);
        offset.put(SourceInfo.ORDER, 1);
        offset.put(SourceInfo.RESUME_TOKEN, "someBase64Token");

        MongoDbOffsetContext context = loader.load(offset);

        assertThat(context.isInitialSnapshotRunning())
                .as("Normal offset (no initsync) should not be flagged as snapshot running")
                .isFalse();
    }

    /**
     * Verify that when initsync=true, the serialized offset roundtrips correctly:
     * getOffset() should contain INITIAL_SYNC=true.
     */
    @Test
    public void loaderInitsyncOffsetShouldRoundtrip() {
        Map<String, Object> originalOffset = new HashMap<>();
        originalOffset.put(SourceInfo.INITIAL_SYNC, true);
        originalOffset.put(SourceInfo.TIMESTAMP, 0);
        originalOffset.put(SourceInfo.ORDER, 0);

        MongoDbOffsetContext context = loader.load(originalOffset);
        Map<String, ?> reserialized = context.getOffset();

        assertThat(reserialized.get(SourceInfo.INITIAL_SYNC))
                .as("Re-serialized offset should preserve initsync=true")
                .isEqualTo(true);
    }

    /**
     * Bug 2 demonstration: When collectionId is null (as it is during an incomplete
     * snapshot), calling getSourceInfo() throws DataException because the "collection"
     * field is required (non-optional) in the schema but the value is null.
     *
     * This documents why MongoDbConnectorTask.validate() uses getOffset() instead of
     * getSourceInfo() for error messages — getSourceInfo() is unsafe when collectionId
     * is null.
     */
    @Test
    public void getSourceInfoThrowsWhenCollectionIsNull() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.INITIAL_SYNC, true);
        offset.put(SourceInfo.TIMESTAMP, 0);
        offset.put(SourceInfo.ORDER, 0);

        MongoDbOffsetContext context = loader.load(offset);

        // getSourceInfo() builds a Struct with required "collection" field = null → crash
        assertThatThrownBy(() -> context.getSourceInfo())
                .as("getSourceInfo() should throw when collectionId is null (required field)")
                .isInstanceOf(DataException.class)
                .hasMessageContaining("collection");
    }

    /**
     * Bug 2 fix verification: getOffset() should work even when collectionId is null,
     * providing a safe alternative for error messages. This is used in
     * MongoDbConnectorTask.validate() instead of the unsafe getSourceInfo().
     */
    @Test
    public void getOffsetWorksWhenCollectionIsNull() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.INITIAL_SYNC, true);
        offset.put(SourceInfo.TIMESTAMP, 0);
        offset.put(SourceInfo.ORDER, 0);

        MongoDbOffsetContext context = loader.load(offset);

        // getOffset() returns a Map, not a Struct, so no schema validation
        assertThatNoException()
                .as("getOffset() should not throw even when collectionId is null")
                .isThrownBy(() -> {
                    Map<String, ?> result = context.getOffset();
                    String safeString = result.toString();
                    assertThat(safeString).isNotEmpty();
                });
    }

    /**
     * Verify that an empty offset context (first start, no stored offset)
     * is NOT flagged as initial snapshot running.
     */
    @Test
    public void emptyOffsetContextShouldNotBeSnapshotRunning() {
        MongoDbOffsetContext context = MongoDbOffsetContext.empty(connectorConfig);

        assertThat(context.isInitialSnapshotRunning())
                .as("Empty (fresh) offset context should not be flagged as snapshot running")
                .isFalse();
    }

    /**
     * Verify that a context where snapshot was explicitly started reports correctly.
     */
    @Test
    public void explicitStartSnapshotShouldBeRecognized() {
        MongoDbOffsetContext context = MongoDbOffsetContext.empty(connectorConfig);
        context.startInitialSnapshot();

        assertThat(context.isInitialSnapshotRunning())
                .as("Explicitly started snapshot should be recognized")
                .isTrue();
    }

    /**
     * Verify that a context with initsync=true has a null resume token,
     * which is what triggers the misleading "offset no longer available" error.
     */
    @Test
    public void initsyncOffsetShouldHaveNullResumeToken() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.INITIAL_SYNC, true);
        offset.put(SourceInfo.TIMESTAMP, 0);
        offset.put(SourceInfo.ORDER, 0);

        MongoDbOffsetContext context = loader.load(offset);

        assertThat(context.lastResumeToken())
                .as("Incomplete snapshot offset should have null resume token")
                .isNull();

        assertThat(context.lastResumeTokenDoc())
                .as("Incomplete snapshot offset should have null resume token doc")
                .isNull();
    }
}
