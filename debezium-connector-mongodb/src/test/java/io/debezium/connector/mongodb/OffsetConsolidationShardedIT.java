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
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
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
     * Contains all events consumed from both shards
     */
    private final SortedSet<ChangeStreamDocument<BsonDocument>> events = new TreeSet<>(EVENT_COMPARATOR);
    /**
     * Guaranteed to contain at least 2 events consumed from shard1
     */
    private final List<ChangeStreamDocument<BsonDocument>> events1 = new ArrayList<>();

    /**
     * Guaranteed to contain at least 2 events consumed from shard0
     */
    private final List<ChangeStreamDocument<BsonDocument>> events0 = new ArrayList<>();

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
            var stream0 = shard0.watch(stages, BsonDocument.class);
            var stream1 = shard1.watch(stages, BsonDocument.class);
            var counter = 0;

            try (var c0 = stream0.cursor(); var c1 = stream1.cursor()) {
                while (events0.size() < 2 || events1.size() < 2) {

                    // insert document through router
                    router
                            .getDatabase(shardedDatabase())
                            .getCollection(shardedCollection())
                            .insertOne(new Document("_id", counter).append("name", "name_" + counter));
                    counter++;

                    // poll each shard change stream
                    var e0 = c0.tryNext();
                    var e1 = c1.tryNext();

                    // add shard events
                    if (e0 != null) {
                        events0.add(e0);
                    }
                    if (e1 != null) {
                        events1.add(e1);
                    }
                }
            }
            events.addAll(events0);
            events.addAll(events1);
        }
    }

    private void storeShardOffsets(List<ChangeStreamDocument<BsonDocument>> shard0, List<ChangeStreamDocument<BsonDocument>> shard1) throws InterruptedException {
        var offsets = new HashMap<Map<String, ?>, Map<String, ?>>();
        Stream.of(
                prepareShardOffsets(mongo.getShard(0), shard0),
                prepareShardOffsets(mongo.getShard(1), shard1))
                .forEach(offsets::putAll);

        storeOffsets(config, offsets);
    }

    private Map<Map<String, ?>, Map<String, ?>> prepareShardOffsets(MongoDbReplicaSet shard, List<ChangeStreamDocument<BsonDocument>> events) {
        if (events.isEmpty()) {
            return Map.of();
        }

        var lastEvent = events.get(events.size() - 2);
        var offset = MongoDbOffsetContext.empty(connectorConfig);
        offset.changeStreamEvent(lastEvent);
        var sourceOffset = offset.getOffset();
        var sourcePartition = Collect.hashMapOf("server_id", TOPIC_PREFIX, "rs", shard.getName());

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
     * @return oldest
     */
    public ChangeStreamDocument<BsonDocument> getOffsetEvent() {
        var event0 = nextToLastElement(events0);
        var event1 = nextToLastElement(events1);

        return EVENT_COMPARATOR.compare(event0, event1) < 0
                ? event0
                : event1;
    }

    public Set<String> getExpectedIds(ChangeStreamDocument<BsonDocument> offsetEvent) {
        // Keep only those events following the offset event
        var following = events.stream()
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

        // Store offsets
        storeShardOffsets(events0, events1);
        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", TOPIC_PREFIX, "streaming", "0");

        // Get ids of expected documents
        var offsetEvent = getOffsetEvent();
        var expected = getExpectedIds(offsetEvent);

        // Assert that all consumed records are expected
        consumeRecordsByTopic(expected.size()).forEach(record -> {
            var id = ((Struct) record.key()).getString("id");
            assertThat(id).isIn(expected);
        });
    }

    @Test
    public void shouldFailToConsolidateOffsetsFromRsModeWhenInvalidationIsNotAllowed() throws InterruptedException {
        Assumptions.assumeThat(mongo.size()).isEqualTo(2);
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbConnectorTask.class);

        var config = this.config.edit()
                .with(MongoDbConnectorConfig.ALLOW_OFFSET_INVALIDATION, false)
                .build();

        // Store offsets
        storeShardOffsets(events0, events1);
        // Start the connector ...
        start(MongoDbConnector.class, config);

        // Assert errors
        logInterceptor.containsErrorMessage("Offset invalidation is not allowed");
        logInterceptor.containsStacktraceElement("Offsets from previous run are invalid, either manually delete");
    }

    @Test
    public void shouldFailToConsolidateOffsetsFromRsModeWhenOneShardOffsetIsMissing() throws InterruptedException {
        Assumptions.assumeThat(mongo.size()).isEqualTo(2);
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbConnectorTask.class);

        // Store only offset for shard0
        storeShardOffsets(events0, List.of());
        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", TOPIC_PREFIX, "streaming", "0");

        // Get ids of expected documents
        var expected = getExpectedIds(events);

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
