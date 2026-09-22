/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Struct;
import org.awaitility.Awaitility;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

import io.debezium.connector.mongodb.connection.DefaultMongoDbAuthProvider;

public class ChangeStreamBatchSizeIT extends AbstractMongoConnectorIT {

    private static final String DATABASE = "batch_size_test";
    private static final String COLLECTION = "events";
    private static final String NAMESPACE = DATABASE + "." + COLLECTION;
    private static final CommandRecorder COMMANDS = new CommandRecorder();

    @BeforeEach
    void setUp() {
        COMMANDS.clear();
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, NAMESPACE)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.CURSOR_MAX_AWAIT_TIME_MS, 100)
                .build();
        try (var client = connect()) {
            client.getDatabase(DATABASE).createCollection(COLLECTION);
        }
    }

    @ParameterizedTest
    @CsvSource({
            "deployment,,",
            "database,batch_size_test,",
            "collection,batch_size_test.events,",
            "deployment,,0",
            "database,batch_size_test,0",
            "collection,batch_size_test.events,0",
            "deployment,,2",
            "database,batch_size_test,2",
            "collection,batch_size_test.events,2"
    })
    void shouldApplyBatchSizeToInitialAndResumedCursors(String scope, String target, Integer fetchSize) {
        final var builder = config.edit().with(MongoDbConnectorConfig.CAPTURE_SCOPE, scope);
        if (target != null) {
            builder.with(MongoDbConnectorConfig.CAPTURE_TARGET, target);
        }
        if (fetchSize != null) {
            builder.with(MongoDbConnectorConfig.QUERY_FETCH_SIZE, fetchSize);
        }
        final var taskContext = new MongoDbTaskContext(builder.build());
        final var recorder = new CommandRecorder();
        final var settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongo.getConnectionString()))
                .addCommandListener(recorder)
                .build();

        try (var client = MongoClients.create(settings)) {
            BsonDocument resumeToken;
            try (var cursor = MongoUtils.openChangeStream(client, taskContext).maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                assertThat(cursor.tryNext()).isNull();
                resumeToken = cursor.getResumeToken();
                assertThat(resumeToken).isNotNull();
            }

            // A backlog ensures the initial aggregate response, as well as getMore, can contain events.
            for (int firstId : new int[]{ 1, 6 }) {
                insertDocuments(DATABASE, COLLECTION, documents(firstId, 5));
                recorder.clear();
                try (var cursor = MongoUtils.openChangeStream(client, taskContext)
                        .resumeAfter(resumeToken)
                        .maxAwaitTime(100, TimeUnit.MILLISECONDS)
                        .cursor()) {
                    List<Integer> ids = new ArrayList<>();
                    Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                        final var event = cursor.tryNext();
                        if (event != null) {
                            ids.add(event.getFullDocument().getInt32("_id").getValue());
                        }
                        assertThat(ids).containsExactlyElementsOf(IntStream.range(firstId, firstId + 5).boxed().toList());
                    });
                    resumeToken = cursor.getResumeToken();
                    // Exercise getMore even when the server default fits the entire backlog in the first batch.
                    assertThat(cursor.tryNext()).isNull();
                }

                final var aggregates = recorder.commands("aggregate");
                assertThat(aggregates).hasSize(1);
                assertBatchSize(aggregates.get(0).getDocument("cursor"), fetchSize);
                assertThat(recorder.commands("getMore")).isNotEmpty()
                        .allSatisfy(command -> assertBatchSize(command, fetchSize));
                if (fetchSize != null && fetchSize > 0) {
                    assertThat(recorder.batchSizes).isNotEmpty()
                            .allSatisfy(size -> assertThat(size).isBetween(0, fetchSize));
                }
            }
        }
    }

    @Test
    void shouldKeepSnapshotFetchSizeIndependentAndResumeAfterRestart() throws InterruptedException {
        config = config.edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_FETCH_SIZE, 3)
                .with(MongoDbConnectorConfig.QUERY_FETCH_SIZE, 2)
                .with(MongoDbConnectorConfig.MAX_BATCH_SIZE, 1)
                .with(MongoDbConnectorConfig.AUTH_PROVIDER_CLASS, MonitoringAuthProvider.class)
                .build();
        insertDocuments(DATABASE, COLLECTION, documents(1, 5));
        start(MongoDbConnector.class, config);
        assertRecords(1, 5, "r");
        waitForStreamingRunning("mongodb", "mongo");

        assertThat(COMMANDS.commands("find"))
                .filteredOn(command -> COLLECTION.equals(command.getString("find").getValue()))
                .isNotEmpty().allSatisfy(command -> assertBatchSize(command, 3));
        assertStreamBatchSize(2);

        insertDocuments(DATABASE, COLLECTION, documents(6, 5));
        assertRecords(6, 5, "c");
        stopConnector();

        insertDocuments(DATABASE, COLLECTION, documents(11, 5));
        COMMANDS.clear();
        start(MongoDbConnector.class, config);
        assertRecords(11, 5, "c");
        waitForStreamingRunning("mongodb", "mongo");
        assertStreamBatchSize(2);
        assertThat(COMMANDS.commands("aggregate")).anySatisfy(command -> assertThat(command.getArray("pipeline")
                .get(0).asDocument().getDocument("$changeStream")).containsKey("resumeAfter"));
        assertNoRecordsToConsume();
    }

    private void assertRecords(int firstId, int count, String operation) throws InterruptedException {
        final var records = consumeRecordsByTopic(count).allRecordsInOrder();
        assertThat(records).hasSize(count).allSatisfy(record -> {
            assertThat(record.topic()).isEqualTo("mongo." + NAMESPACE);
            assertThat(((Struct) record.value()).getString("op")).isEqualTo(operation);
        });
        assertThat(records).extracting(record -> Document.parse(((Struct) record.value()).getString("after")).getInteger("_id"))
                .containsExactlyElementsOf(IntStream.range(firstId, firstId + count).boxed().toList());
    }

    private static void assertStreamBatchSize(int fetchSize) {
        assertThat(COMMANDS.commands("aggregate")).isNotEmpty()
                .allSatisfy(command -> assertBatchSize(command.getDocument("cursor"), fetchSize));
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(COMMANDS.commands("getMore"))
                .filteredOn(command -> "$cmd.aggregate".equals(command.getString("collection").getValue()))
                .isNotEmpty().allSatisfy(command -> assertBatchSize(command, fetchSize)));
    }

    private static void assertBatchSize(BsonDocument command, Integer fetchSize) {
        if (fetchSize == null || fetchSize == 0) {
            assertThat(command).doesNotContainKey("batchSize");
        }
        else {
            assertThat(command).containsKey("batchSize");
            assertThat(command.getNumber("batchSize").intValue()).isEqualTo(fetchSize);
        }
    }

    private static Document[] documents(int firstId, int count) {
        return IntStream.range(firstId, firstId + count).mapToObj(id -> new Document("_id", id)).toArray(Document[]::new);
    }

    public static class MonitoringAuthProvider extends DefaultMongoDbAuthProvider {
        @Override
        public MongoClientSettings.Builder addAuthConfig(MongoClientSettings.Builder settings) {
            return super.addAuthConfig(settings).addCommandListener(COMMANDS);
        }
    }

    private static class CommandRecorder implements CommandListener {
        private final List<BsonDocument> commands = new CopyOnWriteArrayList<>();
        private final List<Integer> batchSizes = new CopyOnWriteArrayList<>();

        @Override
        public void commandStarted(CommandStartedEvent event) {
            if (List.of("aggregate", "getMore", "find").contains(event.getCommandName())) {
                commands.add(event.getCommand().clone());
            }
        }

        @Override
        public void commandSucceeded(CommandSucceededEvent event) {
            final var cursor = event.getResponse().getDocument("cursor", null);
            if (cursor != null) {
                if (cursor.containsKey("firstBatch")) {
                    batchSizes.add(cursor.getArray("firstBatch").size());
                }
                if (cursor.containsKey("nextBatch")) {
                    batchSizes.add(cursor.getArray("nextBatch").size());
                }
            }
        }

        List<BsonDocument> commands(String name) {
            return commands.stream().filter(command -> command.containsKey(name)).toList();
        }

        void clear() {
            commands.clear();
            batchSizes.clear();
        }
    }
}
