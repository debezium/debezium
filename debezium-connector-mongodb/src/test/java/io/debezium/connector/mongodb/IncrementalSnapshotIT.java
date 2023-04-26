/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.fest.assertions.Assertions;
import org.fest.assertions.MapAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.SnapshotMode;
import io.debezium.engine.DebeziumEngine;
import io.debezium.util.Testing;

/**
 * Test to verify incremental snapshotting for MongoDB.
 *
 * @author Jiri Pechanec
 */
public class IncrementalSnapshotIT extends AbstractMongoConnectorIT {

    protected static final int ROW_COUNT = 1_000;
    private static final int MAXIMUM_NO_RECORDS_CONSUMES = 3;

    private static final String DATABASE_NAME = "dbA";
    private static final String COLLECTION_NAME = "c1";
    private static final String FULL_COLLECTION_NAME = DATABASE_NAME + "." + "c1";

    private static final String DOCUMENT_ID = "_id";

    protected static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-is.txt")
            .toAbsolutePath();

    @Rule
    public TestRule skipForOplog = new SkipForOplogTestRule();

    @Before
    public void before() {
        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config().build());

        TestHelper.cleanDatabase(primary(), DATABASE_NAME);
    }

    @After
    public void after() {
        TestHelper.cleanDatabase(primary(), DATABASE_NAME);
    }

    protected Class<MongoDbConnector> connectorClass() {
        return MongoDbConnector.class;
    }

    protected Configuration.Builder config() {
        return TestHelper.getConfiguration()
                .edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE_NAME)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, fullDataCollectionName() + ",dbA.c1,dbA.signals")
                .with(MongoDbConnectorConfig.SIGNAL_DATA_COLLECTION, "dbA.signals")
                .with(MongoDbConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER);
    }

    protected String dataCollectionName() {
        return COLLECTION_NAME;
    }

    protected String fullDataCollectionName() {
        return FULL_COLLECTION_NAME;
    }

    protected String topicName() {
        return "mongo1" + "." + fullDataCollectionName();
    }

    protected void populateDataCollection(MongoPrimary connection, String dataCollectionName) {
        final Document[] documents = new Document[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            final Document doc = new Document();
            doc.append(DOCUMENT_ID, i + 1).append("aa", i);
            documents[i] = doc;
        }
        insertDocumentsInTx(DATABASE_NAME, dataCollectionName, documents);
    }

    protected void populateDataCollection(MongoPrimary connection) {
        populateDataCollection(connection, dataCollectionName());
    }

    protected void populateDataCollection() {
        populateDataCollection(primary());
    }

    protected void insertAdditionalData() {
        final Document[] documents = new Document[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            final Document doc = new Document();
            doc.append(DOCUMENT_ID, i + ROW_COUNT + 1).append("aa", i + ROW_COUNT);
            documents[i] = doc;
        }
        insertDocumentsInTx(DATABASE_NAME, COLLECTION_NAME, documents);
    }

    protected void updateData(int batchSize) {

        for (int i = 0; i < ROW_COUNT / batchSize; i++) {
            final Document gt = new Document();
            gt.append("$gt", i * batchSize);

            final Document lte = new Document();
            lte.append("$lte", (i + 1) * batchSize);

            final Document filter = new Document();
            filter.append("$and", Arrays.asList(
                    (new Document()).append(DOCUMENT_ID, gt),
                    (new Document()).append(DOCUMENT_ID, lte)));

            final Document update = new Document();
            update.append("$inc", (new Document()).append("aa", 2000));

            updateDocumentsInTx(DATABASE_NAME, COLLECTION_NAME, filter, update);
        }
    }

    protected void startConnector(DebeziumEngine.CompletionCallback callback) {
        startConnector(Function.identity(), callback);
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        startConnector(custConfig, loggingCompletion());
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

    protected void sendAdHocSnapshotSignal(String... dataCollectionIds) throws SQLException {
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> "\\\"" + x + "\\\"")
                .collect(Collectors.joining(", "));
        insertDocuments("dbA", "signals",
                new Document[]{ Document.parse("{\"type\": \"execute-snapshot\", \"payload\": \"{\\\"data-collections\\\": [" + dataCollectionIdsList + "]}\"}") });
    }

    protected void sendAdHocSnapshotSignal() throws SQLException {
        sendAdHocSnapshotSignal(fullDataCollectionName());
    }

    protected Map<Integer, Integer> consumeMixedWithIncrementalSnapshot(int recordCount) throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, this::extractFieldValue, x -> true, null);
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
                                                                      Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, dataCompleted,
                k -> Integer.parseInt(k.getString(pkFieldName())), valueConverter, topicName(), recordConsumer);
    }

    protected <V> Map<Integer, V> consumeMixedWithIncrementalSnapshot(int recordCount,
                                                                      Predicate<Map.Entry<Integer, V>> dataCompleted,
                                                                      Function<Struct, Integer> idCalculator,
                                                                      Function<SourceRecord, V> valueConverter,
                                                                      String topicName,
                                                                      Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        final Map<Integer, V> dbChanges = new HashMap<>();
        int noRecords = 0;
        for (;;) {
            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SourceRecord> dataRecords = records.recordsForTopic(topicName);
            if (records.allRecordsInOrder().isEmpty()) {
                noRecords++;
                Assertions.assertThat(noRecords).describedAs(String.format("Too many no data record results, %d < %d", dbChanges.size(), recordCount))
                        .isLessThanOrEqualTo(MAXIMUM_NO_RECORDS_CONSUMES);
                continue;
            }
            noRecords = 0;
            if (dataRecords == null || dataRecords.isEmpty()) {
                continue;
            }
            dataRecords.forEach(record -> {
                final int id = idCalculator.apply((Struct) record.key());
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

        Assertions.assertThat(dbChanges).hasSize(recordCount);
        return dbChanges;
    }

    protected Map<Integer, SourceRecord> consumeRecordsMixedWithIncrementalSnapshot(int recordCount) throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, Function.identity(), x -> true, null);
    }

    protected Map<Integer, Integer> consumeMixedWithIncrementalSnapshot(int recordCount, Predicate<Map.Entry<Integer, Integer>> dataCompleted,
                                                                        Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, this::extractFieldValue, dataCompleted, recordConsumer);
    }

    protected Map<Integer, SourceRecord> consumeRecordsMixedWithIncrementalSnapshot(int recordCount, Predicate<Map.Entry<Integer, SourceRecord>> dataCompleted,
                                                                                    Consumer<List<SourceRecord>> recordConsumer)
            throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount, Function.identity(), dataCompleted, recordConsumer);
    }

    protected String valueFieldName() {
        return "aa";
    }

    protected String pkFieldName() {
        return "id";
    }

    @Test
    public void snapshotOnly() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();
        startConnector();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void invalidTablesInTheList() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();
        startConnector();

        sendAdHocSnapshotSignal("dbA.invalid1", fullDataCollectionName(), "invalid2");

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void snapshotOnlyWithRestart() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();
        final Configuration config = config().build();
        startAndConsumeTillEnd(connectorClass(), config);
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final AtomicInteger recordCounter = new AtomicInteger();
        final AtomicBoolean restarted = new AtomicBoolean();
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount, x -> true,
                x -> {
                    if (recordCounter.addAndGet(x.size()) > 50 && !restarted.get()) {
                        stopConnector();
                        assertConnectorNotRunning();

                        start(connectorClass(), config);
                        waitForConnectorToStart();
                        restarted.set(true);
                    }
                });
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void inserts() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();
        startConnector();

        sendAdHocSnapshotSignal();

        insertAdditionalData();

        final int expectedRecordCount = ROW_COUNT * 2;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }

    @Test
    public void updates() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();
        startConnector();

        sendAdHocSnapshotSignal();

        updateData(10);

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount,
                x -> x.getValue() >= 2000, null);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i + 2000));
        }
    }

    @Test
    public void updatesWithRestart() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();
        final Configuration config = config().build();
        startAndConsumeTillEnd(connectorClass(), config);
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignal();

        updateData(10);

        final int expectedRecordCount = ROW_COUNT;
        final AtomicInteger recordCounter = new AtomicInteger();
        final AtomicBoolean restarted = new AtomicBoolean();
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount,
                x -> x.getValue() >= 2000, x -> {
                    if (recordCounter.addAndGet(x.size()) > 50 && !restarted.get()) {
                        stopConnector();
                        assertConnectorNotRunning();

                        start(connectorClass(), config);
                        waitForConnectorToStart();
                        restarted.set(true);
                    }
                });
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i + 2000));
        }
    }

    @Test
    public void updatesLargeChunk() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();
        startConnector(x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, ROW_COUNT));

        sendAdHocSnapshotSignal();

        updateData(ROW_COUNT);

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount,
                x -> x.getValue() >= 2000, null);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i + 2000));
        }
    }

    @Override
    protected int getMaximumEnqueuedRecordCount() {
        return ROW_COUNT * 3;
    }
}
