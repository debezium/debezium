/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.assertj.core.api.Assumptions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.connection.ConnectionStrings;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Collect;

public class OffsetConsolidationShardedIT extends AbstractShardedMongoConnectorIT {

    public static final String TOPIC_PREFIX = "mongo";

    /**
     * Compares change event by cluster time
     */
    private static final Comparator<ChangeStreamDocument<BsonDocument>> EVENT_COMPARATOR = Comparator.comparing(ChangeStreamDocument::getClusterTime);

    public Configuration config;
    private MongoDbConnectorConfig connectorConfig;

    /**
     * Contains all events consumed from router (always at least 4)
     */
    private final SortedSet<ChangeStreamDocument<BsonDocument>> allRouterEvents = new TreeSet<>(EVENT_COMPARATOR);

    /**
     * Guaranteed to contain all consumed from shard0 (always at least 2)
     */
    private final List<ChangeStreamDocument<BsonDocument>> shardEvents0 = new ArrayList<>();

    /**
     * Guaranteed to contain all consumed from shard1 (always at least 2)
     */
    private final List<ChangeStreamDocument<BsonDocument>> shardEvents1 = new ArrayList<>();

    @Before
    public void setupDatabase() {
        Assumptions.assumeThat(mongo.size()).isEqualTo(2);

        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(MongoDbConnectorConfig.ALLOW_OFFSET_INVALIDATION, true)
                .build();
        connectorConfig = new MongoDbConnectorConfig(config);
        var pipelineFactory = new ChangeStreamPipelineFactory(connectorConfig, new Filters.FilterConfig(config));
        insertInitialDocuments(pipelineFactory.create());
    }

    public void insertInitialDocuments(ChangeStreamPipeline pipeline) {
        var stages = pipeline.getStages();

        // Insert documents until each shard has at least 2
        try (var router = connect(); var shard0 = connect(mongo.getShard(0)); var shard1 = connect(mongo.getShard(1))) {
            var routerStream = router.watch(stages, BsonDocument.class);
            var shardStream0 = shard0.watch(stages, BsonDocument.class);
            var shardStream1 = shard1.watch(stages, BsonDocument.class);
            var counter = 0;

            try (var rc = routerStream.cursor(); var sc0 = shardStream0.cursor(); var sc1 = shardStream1.cursor()) {
                while (shardEvents0.size() < 2 || shardEvents1.size() < 2) {
                    // insert document through router
                    router
                            .getDatabase(shardedDatabase())
                            .getCollection(shardedCollection())
                            .insertOne(new Document("_id", counter).append("name", "name_" + counter));
                    counter++;

                    // poll each change stream for an event
                    // > router is guaranteed to receive the event
                    // > one of the shards will also receive the event
                    var r = rc.next();
                    var s0 = sc0.tryNext();
                    var s1 = sc1.tryNext();

                    // Cache received events
                    if (s0 != null) {
                        shardEvents0.add(s0);
                    }
                    if (s1 != null) {
                        shardEvents1.add(s1);
                    }
                    allRouterEvents.add(r);
                }
            }
        }
    }

    private void storeReplicaSetModeOffsets(List<ChangeStreamDocument<BsonDocument>> shard0, List<ChangeStreamDocument<BsonDocument>> shard1)
            throws InterruptedException {
        var offsets = new HashMap<Map<String, ?>, Map<String, ?>>();
        Stream.of(
                prepareReplicaSetOffset(mongo.getShard(0).getName(), shard0),
                prepareReplicaSetOffset(mongo.getShard(1).getName(), shard1))
                .forEach(offsets::putAll);

        storeOffsets(config, offsets);
    }

    private void storeShardedModeOffsets(List<ChangeStreamDocument<BsonDocument>> events) throws InterruptedException {
        var offsets = prepareReplicaSetOffset(ConnectionStrings.CLUSTER_RS_NAME, events);
        storeOffsets(config, offsets);
    }

    private Map<Map<String, ?>, Map<String, ?>> prepareReplicaSetOffset(String rsName, List<ChangeStreamDocument<BsonDocument>> events) {
        if (events.isEmpty()) {
            return Map.of();
        }

        var lastEvent = events.get(events.size() - 2);
        var offset = MongoDbOffsetContext.empty(connectorConfig);
        offset.changeStreamEvent(lastEvent);
        var sourceOffset = offset.getOffset();
        var sourcePartition = Collect.hashMapOf("server_id", TOPIC_PREFIX, "rs", rsName);

        return Map.of(sourcePartition, sourceOffset);
    }

    public static <T> T nextToLastElement(List<T> collection) {
        if (collection.size() < 2) {
            throw new IndexOutOfBoundsException("At least 2 elements required");
        }
        return collection.get(collection.size() - 2);
    }

    /**
     * Gets the older out of the two next-to-last events consumed by shard specific change streams.
     *
     * @return offset event
     */
    public ChangeStreamDocument<BsonDocument> getOffsetEvent(List<ChangeStreamDocument<BsonDocument>> shard0, List<ChangeStreamDocument<BsonDocument>> shard1) {
        var event0 = nextToLastElement(shard0);
        var event1 = nextToLastElement(shard1);

        return EVENT_COMPARATOR.compare(event0, event1) < 0
                ? event0
                : event1;
    }

    /**
     * Gets  next-to-last event consumed by router change streams.
     *
     * @return offset event
     */
    public ChangeStreamDocument<BsonDocument> getOffsetEvent(List<ChangeStreamDocument<BsonDocument>> events) {
        return nextToLastElement(events);
    }

    public Set<String> getExpectedIds(ChangeStreamDocument<BsonDocument> offsetEvent) {
        // Keep only those events following the offset event
        var following = allRouterEvents.stream()
                .dropWhile(e -> EVENT_COMPARATOR.compare(offsetEvent, e) >= 0)
                .collect(Collectors.toList());

        return getExpectedIds(following);
    }

    public Set<String> getExpectedIds(Collection<ChangeStreamDocument<BsonDocument>> events) {
        return events.stream()
                .map(e -> e.getDocumentKey())
                .map(k -> k.getInt32("_id"))
                .map(BsonInt32::getValue)
                .map(String::valueOf)
                .collect(Collectors.toSet());
    }

    @Test
    public void shouldConsolidateOffsetsFromRsMode() throws InterruptedException {
        Assumptions.assumeThat(mongo.size()).isEqualTo(2);
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbConnectorTask.class);

        // Store offsets
        storeReplicaSetModeOffsets(shardEvents0, shardEvents1);
        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", TOPIC_PREFIX, "streaming", "0");

        // Get ids of expected documents
        var offsetEvent = getOffsetEvent(shardEvents0, shardEvents1);
        var expected = getExpectedIds(offsetEvent);

        // Assert that all consumed records are expected
        var records = consumeRecordsByTopic(expected.size());
        assertThat(records.allRecordsInOrder()).hasSameSizeAs(expected);
        records.forEach(record -> {
            verifyNotFromInitialSync(record);
            var id = ((Struct) record.key()).getString("id");
            assertThat(id).isIn(expected);
        });

        // Assert messages
        logInterceptor.containsMessage("checking shard specific offsets");

    }

    @Test
    public void shouldUseOffsetsFromShardedMode() throws InterruptedException {
        Assumptions.assumeThat(mongo.size()).isEqualTo(2);
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbConnectorTask.class);
        var events = new ArrayList<>(allRouterEvents);

        // Store offsets
        storeShardedModeOffsets(events);
        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", TOPIC_PREFIX, "streaming", "0");

        // Get ids of expected documents
        var offsetEvent = getOffsetEvent(events);
        var expected = getExpectedIds(offsetEvent);

        // Assert that all consumed records are expected
        var records = consumeRecordsByTopic(expected.size());
        assertThat(records.allRecordsInOrder()).hasSameSizeAs(expected);
        records.forEach(record -> {
            verifyNotFromInitialSync(record);
            var id = ((Struct) record.key()).getString("id");
            assertThat(id).isIn(expected);
        });

        // Asset info message
        logInterceptor.containsMessage("Found compatible offset from previous version");
    }

    @Test
    public void shouldFailToConsolidateOffsetsFromRsModeWhenInvalidationIsNotAllowed() throws InterruptedException {
        Assumptions.assumeThat(mongo.size()).isEqualTo(2);
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbConnectorTask.class);

        var config = this.config.edit()
                .with(MongoDbConnectorConfig.ALLOW_OFFSET_INVALIDATION, false)
                .build();

        // Store offsets
        storeReplicaSetModeOffsets(shardEvents0, shardEvents1);
        // Start the connector ...
        start(MongoDbConnector.class, config);

        // Assert errors
        logInterceptor.containsErrorMessage("Offset invalidation is not allowed");
        logInterceptor.containsStacktraceElement("Offsets from previous version are invalid, either manually delete");
    }

    @Test
    public void shouldFailToConsolidateOffsetsFromRsModeWhenOneShardOffsetIsMissing() throws InterruptedException {
        Assumptions.assumeThat(mongo.size()).isEqualTo(2);
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbConnectorTask.class);

        // Store only offset for shard0
        storeReplicaSetModeOffsets(shardEvents0, List.of());
        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", TOPIC_PREFIX, "streaming", "0");

        // Get ids of expected documents
        var expected = getExpectedIds(allRouterEvents);

        // Assert warning message
        logInterceptor.containsErrorMessage("At least one shard is missing previously recorded offset, so empty offset will be use");

        // Assert that all expected records were consumed
        var records = consumeRecordsByTopic(expected.size());
        assertThat(records.allRecordsInOrder()).hasSameSizeAs(expected);
        var foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            verifyFromInitialSync(record, foundLast);
            var id = ((Struct) record.key()).getString("id");
            assertThat(id).isIn(expected);
        });
    }
}
