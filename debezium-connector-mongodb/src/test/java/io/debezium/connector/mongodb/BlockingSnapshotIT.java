/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.engine.DebeziumEngine;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;

public class BlockingSnapshotIT extends AbstractMongoConnectorIT {

    protected static final int ROW_COUNT = 1_000;

    private static final String DATABASE_NAME = "dbA";
    private static final String COLLECTION_NAME = "c1";
    private static final String SIGNAL_COLLECTION_NAME = DATABASE_NAME + ".signals";
    private static final String FULL_COLLECTION_NAME = DATABASE_NAME + "." + COLLECTION_NAME;

    private static final String DOCUMENT_ID = "_id";

    @Before
    public void before() {
        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config().build());

        TestHelper.cleanDatabase(mongo, DATABASE_NAME);
    }

    @After
    public void after() {
        TestHelper.cleanDatabase(mongo, DATABASE_NAME);
    }

    @Test
    public void executeBlockingSnapshot() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();

        startConnector(Function.identity());

        waitForSnapshotToBeCompleted("mongodb", "mongo1", "0", null);

        insertRecords(ROW_COUNT, ROW_COUNT);

        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT * 2);

        sendAdHocBlockingSnapshotSignal(fullDataCollectionName());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        assertRecordsFromSnapshotAndStreamingArePresent((ROW_COUNT * 2) + 1);

        insertRecords(ROW_COUNT, (ROW_COUNT * 2));

        int signalingRecords = 1;

        assertStreamingRecordsArePresent(ROW_COUNT + signalingRecords);

    }

    @Test
    public void executeBlockingSnapshotWhileStreaming() throws Exception {
        // Testing.Debug.enable();

        populateDataCollection();

        startConnector(Function.identity());

        waitForSnapshotToBeCompleted("mongodb", "mongo1", "0", null);

        Future<?> batchInserts = executeAsync(insertTask());

        Thread.sleep(2000); // Let's start stream some insert

        sendAdHocBlockingSnapshotSignal(fullDataCollectionName());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        waitForStreamingRunning("mongodb", "mongo1", getStreamingNamespace(), "0");

        Long totalSnapshotRecords = getTotalSnapshotRecords(replicaSetFullDataCollectionName(), "mongodb", "mongo1", "0", null);

        batchInserts.get(120, TimeUnit.SECONDS);

        insertRecords(ROW_COUNT, (ROW_COUNT * 2));

        int signalingRecords = 1 + // from streaming
                1; // from snapshot

        assertRecordsWithValuesPresent((int) ((ROW_COUNT * 3) + totalSnapshotRecords + signalingRecords),
                getExpectedValues(totalSnapshotRecords));
    }

    protected Class<MongoDbConnector> connectorClass() {
        return MongoDbConnector.class;
    }

    protected Configuration.Builder config() {
        return TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE_NAME)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, fullDataCollectionName())
                .with(MongoDbConnectorConfig.SIGNAL_DATA_COLLECTION, SIGNAL_COLLECTION_NAME)
                .with(MongoDbConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(MongoDbConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL);
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

    protected void populateDataCollection(String dataCollectionName) {
        final Document[] documents = new Document[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            final Document doc = new Document();
            doc.append(DOCUMENT_ID, i + 1).append("aa", i);
            documents[i] = doc;
        }
        insertDocumentsInTx(DATABASE_NAME, dataCollectionName, documents);
    }

    protected void populateDataCollection() {
        populateDataCollection(dataCollectionName());
    }

    protected int insertMaxSleep() {
        return 100;
    }

    private Runnable insertTask() {
        return () -> {
            try {
                insertRecordsWithRandomSleep(ROW_COUNT, ROW_COUNT, insertMaxSleep());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Long getTotalSnapshotRecords(String table, String connector, String server, String task, String database) throws MalformedObjectNameException,
            ReflectionException, AttributeNotFoundException, InstanceNotFoundException,
            MBeanException {

        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        Map<String, Object> rowsScanned = (Map<String, Object>) mbeanServer.getAttribute(getSnapshotMetricsObjectName(connector, server, task, database),
                "RowsScanned");

        String unquotedTableName = table.replace("`", "");
        return (Long) rowsScanned.get(unquotedTableName);
    }

    private static List<Integer> getExpectedValues(Long totalSnapshotRecords) {

        List<Integer> initialSnapShotValues = IntStream.rangeClosed(0, 999).boxed().collect(Collectors.toList());
        List<Integer> firstStreamingBatchValues = IntStream.rangeClosed(1000, 1999).boxed().collect(Collectors.toList());
        List<Integer> blockingSnapshotValues = Stream.of(
                initialSnapShotValues,
                IntStream.rangeClosed(1000, Math.toIntExact(totalSnapshotRecords)).boxed().collect(Collectors.toList())).flatMap(List::stream)
                .collect(Collectors.toList());
        List<Integer> secondStreamingBatchValues = IntStream.rangeClosed(2000, 2999).boxed().collect(Collectors.toList());
        return Stream.of(initialSnapShotValues, firstStreamingBatchValues, blockingSnapshotValues, secondStreamingBatchValues).flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private static void waitForLogMessage(String message, Class<?> logEmitterClass) {
        LogInterceptor interceptor = new LogInterceptor(logEmitterClass);
        Awaitility.await()
                .alias("Snapshot not completed on time")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30L, TimeUnit.SECONDS)
                .until(() -> interceptor.containsMessage(message));
    }

    private Future<?> executeAsync(Runnable operation) {
        return Executors.newSingleThreadExecutor().submit(operation);
    }

    private void assertStreamingRecordsArePresent(int expectedRecords) throws InterruptedException {

        assertRecordsWithValuesPresent(expectedRecords, IntStream.range(2000, 2999).boxed().collect(Collectors.toList()));
    }

    private void assertRecordsFromSnapshotAndStreamingArePresent(int expectedRecords) throws InterruptedException {

        assertRecordsWithValuesPresent(expectedRecords, IntStream.range(0, expectedRecords - 2).boxed().collect(Collectors.toList()));
    }

    private void assertRecordsWithValuesPresent(int expectedRecords, List<Integer> expectedValues) throws InterruptedException {

        SourceRecords snapshotAndStreamingRecords = consumeRecordsByTopic(expectedRecords, 10);
        assertThat(snapshotAndStreamingRecords.allRecordsInOrder().size()).isEqualTo(expectedRecords);
        List<Integer> actual = snapshotAndStreamingRecords.recordsForTopic(topicName()).stream()
                .map(record -> extractFieldValue(record, "aa"))
                .collect(Collectors.toList());
        assertThat(actual).containsAll(expectedValues);
    }

    protected Integer extractFieldValue(SourceRecord record, String fieldName) {
        final String after = ((Struct) record.value()).getString("after");
        final Pattern p = Pattern.compile("\"" + fieldName + "\": (\\d+)");
        final Matcher m = p.matcher(after);
        m.find();
        return Integer.parseInt(m.group(1));
    }

    private void insertRecords(int rowCount, int startingPkId) {

        final Document[] documents = new Document[ROW_COUNT];
        for (int i = 0; i < rowCount; i++) {
            final Document doc = new Document();
            doc.append(DOCUMENT_ID, i + startingPkId + 1).append("aa", i + startingPkId);
            documents[i] = doc;
        }
        insertDocumentsInTx(DATABASE_NAME, COLLECTION_NAME, documents);
    }

    private void insertRecordsWithRandomSleep(int rowCount, int startingPkId, int maxSleep, Runnable actionOnInsert) {

        try {
            for (int i = 0; i < rowCount; i++) {
                final Document doc = new Document();
                doc.append(DOCUMENT_ID, i + startingPkId + 1).append("aa", i + startingPkId);

                insertDocuments(DATABASE_NAME, COLLECTION_NAME, doc);

                actionOnInsert.run();

                int sleepTime = ThreadLocalRandom.current().nextInt(1, maxSleep);
                Thread.sleep(sleepTime);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void insertRecordsWithRandomSleep(int rowCount, int startingPkId, int maxSleep) {

        insertRecordsWithRandomSleep(rowCount, startingPkId, maxSleep, () -> {
        });
    }

    protected void sendAdHocBlockingSnapshotSignal(String... dataCollectionIds) {
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> "\"" + x + "\"")
                .collect(Collectors.joining(", "));
        insertDocuments("dbA", "signals",
                Document.parse("{\"type\": \"execute-snapshot\", \"payload\": {\"type\": \"BLOCKING\",\"data-collections\": [" + dataCollectionIdsList + "]}}"));
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        startConnector(custConfig, loggingCompletion());
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig, DebeziumEngine.CompletionCallback callback) {
        final Configuration config = custConfig.apply(config()).build();
        start(connectorClass(), config, callback);

        waitForAvailableRecords(5, TimeUnit.SECONDS);
    }

    @NotNull
    private String replicaSetFullDataCollectionName() {
        return "rs0." + fullDataCollectionName();
    }

}
