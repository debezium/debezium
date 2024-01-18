/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.engine.DebeziumEngine;

public class ShardedIncrementalSnapshotIT extends AbstractShardedMongoConnectorIT {
    protected static final int ROW_COUNT = 1_000;
    private static final int MAXIMUM_NO_RECORDS_CONSUMES = 3;
    private static final String DATABASE_NAME = "dbA";
    private static final String COLLECTION_NAME = "c1";
    private static final String SIGNAL_COLLECTION_NAME = DATABASE_NAME + ".signals";
    private static final String FULL_COLLECTION_NAME = DATABASE_NAME + "." + COLLECTION_NAME;
    private static final String DOCUMENT_ID = "_id";
    private static final int CHUNK_SIZE = 100;

    @Override
    protected String shardedDatabase() {
        return DATABASE_NAME;
    }

    @Override
    protected Map<String, String> shardedCollections() {
        return Map.of(COLLECTION_NAME, DOCUMENT_ID);
    }

    protected String fullDataCollectionName() {
        return FULL_COLLECTION_NAME;
    }

    protected String topicName() {
        return "mongo1" + "." + fullDataCollectionName();
    }

    protected Configuration.Builder config() {
        return TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, shardedDatabase())
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, fullDataCollectionName() + ",dbA.c1,dbA.c2")
                .with(MongoDbConnectorConfig.SIGNAL_DATA_COLLECTION, SIGNAL_COLLECTION_NAME)
                .with(MongoDbConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, CHUNK_SIZE)
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NEVER);
    }

    @Test
    public void snapshotOnlyWithInt64() throws Exception {
        long firstKey = Integer.MAX_VALUE + 1L;
        snapshotOnly(firstKey, k -> k + 1);
    }

    private <K> void snapshotOnly(K initialId, Function<K, K> idGenerator) throws Exception {
        final Map<K, Document> documents = new LinkedHashMap<>();

        K key = initialId;
        for (int i = 0; i < ROW_COUNT; i++) {
            final Document doc = new Document();
            doc.append(DOCUMENT_ID, key).append(valueFieldName(), i);
            documents.put(key, doc);
            key = idGenerator.apply(key);
        }
        insertDocumentsInTx(DATABASE_NAME, COLLECTION_NAME, documents.values().toArray(Document[]::new));

        startConnector();
        sendAdHocSnapshotSignal();

        final var dbChanges = consumeMixedWithIncrementalSnapshot(
                ROW_COUNT,
                x -> true,
                k -> k.getString(pkFieldName()),
                this::extractFieldValue,
                topicName(), null);

        var serialization = new JsonSerialization();

        var expected = documents.values()
                .stream()
                .map(d -> d.toBsonDocument())
                .collect(toMap(
                        d -> serialization.getDocumentId(d),
                        d -> d.getInt32(valueFieldName()).getValue()));

        assertThat(dbChanges).containsAllEntriesOf(expected);
    }

    protected String valueFieldName() {
        return "aa";
    }

    protected String pkFieldName() {
        return "id";
    }

    protected Class<MongoDbConnector> connectorClass() {
        return MongoDbConnector.class;
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig, DebeziumEngine.CompletionCallback callback) {
        final Configuration config = custConfig.apply(config()).build();
        start(connectorClass(), config, callback);
        waitForConnectorToStart();

        waitForAvailableRecords(5, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();
    }

    protected void startConnector() {
        startConnector(Function.identity(), loggingCompletion());
    }

    protected void waitForConnectorToStart() {
        assertConnectorIsRunning();
    }

    protected void sendAdHocSnapshotSignal() throws SQLException {
        sendAdHocSnapshotSignal(fullDataCollectionName());
    }

    protected void sendAdHocSnapshotSignal(String... dataCollectionIds) throws SQLException {
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> "\\\"" + x + "\\\"")
                .collect(Collectors.joining(", "));
        insertDocuments("dbA", "signals",
                new Document[]{ Document.parse("{\"type\": \"execute-snapshot\", \"payload\": \"{\\\"data-collections\\\": [" + dataCollectionIdsList + "]}\"}") });
    }

    protected Map<Integer, Integer> consumeMixedWithIncrementalSnapshot(int recordCount, String topicName) throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, this::extractFieldValue, x -> true, null, topicName);
    }

    protected Integer extractFieldValue(SourceRecord record) {
        final String after = ((Struct) record.value()).getString("after");
        final Pattern p = Pattern.compile("\"" + valueFieldName() + "\": (\\d+)");
        final Matcher m = p.matcher(after);
        m.find();
        return Integer.parseInt(m.group(1));
    }

    protected <V> Map<Integer, V> consumeMixedWithIncrementalSnapshot(int recordCount, Function<SourceRecord, V> valueConverter,
                                                                      Predicate<Map.Entry<Integer, V>> dataCompleted,
                                                                      Consumer<List<SourceRecord>> recordConsumer,
                                                                      String topicName)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, dataCompleted,
                k -> Integer.parseInt(k.getString(pkFieldName())), valueConverter, topicName, recordConsumer);
    }

    protected <V, K> Map<K, V> consumeMixedWithIncrementalSnapshot(int recordCount,
                                                                   Predicate<Map.Entry<K, V>> dataCompleted,
                                                                   Function<Struct, K> idCalculator,
                                                                   Function<SourceRecord, V> valueConverter,
                                                                   String topicName,
                                                                   Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        final Map<K, V> dbChanges = new HashMap<>();
        int noRecords = 0;
        for (;;) {
            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SourceRecord> dataRecords = records.recordsForTopic(topicName);
            if (records.allRecordsInOrder().isEmpty()) {
                noRecords++;
                assertThat(noRecords).describedAs(String.format("Too many no data record results, %d < %d", dbChanges.size(), recordCount))
                        .isLessThanOrEqualTo(MAXIMUM_NO_RECORDS_CONSUMES);
                continue;
            }
            noRecords = 0;
            if (dataRecords == null || dataRecords.isEmpty()) {
                continue;
            }
            dataRecords.forEach(record -> {
                final K id = idCalculator.apply((Struct) record.key());
                final V value = valueConverter.apply(record);
                dbChanges.put(id, value);
            });
            if (recordConsumer != null) {
                recordConsumer.accept(dataRecords);
            }
            if (dbChanges.size() >= recordCount) {
                if (!dbChanges.entrySet().stream().anyMatch(dataCompleted.negate())) {
                    break;
                }
            }
        }

        assertThat(dbChanges).hasSize(recordCount);
        return dbChanges;
    }
}
