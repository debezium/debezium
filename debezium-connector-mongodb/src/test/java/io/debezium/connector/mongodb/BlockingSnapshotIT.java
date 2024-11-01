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
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.TabularDataSupport;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.engine.DebeziumEngine;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;

public class BlockingSnapshotIT extends AbstractMongoConnectorIT {

    protected static final int ROW_COUNT = 1_000;

    private static final String DATABASE_NAME = "dbA";
    private static final String COLLECTION_NAME = "c1";

    private static final String COLLECTION2_NAME = "c2";
    private static final String SIGNAL_COLLECTION_NAME = DATABASE_NAME + ".signals";
    private static final String FULL_COLLECTION_NAME = DATABASE_NAME + "." + COLLECTION_NAME;

    private static final String FULL_COLLECTION2_NAME = DATABASE_NAME + "." + COLLECTION2_NAME;

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

        assertStreamingRecordsArePresent(ROW_COUNT);

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

        Long totalSnapshotRecords = getTotalSnapshotRecords(fullDataCollectionName(), "mongodb", "mongo1", "0", null);

        batchInserts.get(120, TimeUnit.SECONDS);

        insertRecords(ROW_COUNT, (ROW_COUNT * 2));

        int signalingRecords = 1; // from streaming

        assertRecordsWithValuesPresent((int) ((ROW_COUNT * 3) + totalSnapshotRecords + signalingRecords),
                getExpectedValues(totalSnapshotRecords), topicName());
    }

    @Test
    public void executeBlockingSnapshotWithAdditionalCondition() throws Exception {
        // Testing.Print.enable();

        populateDataCollection(dataCollectionNames().get(1));

        startConnector(Function.identity());

        waitForStreamingRunning("mongodb", "mongo1", getStreamingNamespace(), "0");

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(
                Map.of(fullDataCollectionNames().get(1), "{ aa: { $lt: 500 } }"),
                fullDataCollectionNames().get(1));

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        int signalingRecords = 1; // from streaming

        assertRecordsWithValuesPresent(500 + signalingRecords, IntStream.rangeClosed(0, 499).boxed().collect(Collectors.toList()), topicNames().get(1));

    }

    @FixFor("DBZ-7903")
    @Test
    public void aFailedBlockingSnapshotShouldNotCauseInitialSnapshotOnRestart() throws Exception {
        // Testing.Print.enable();

        populateDataCollection(dataCollectionNames().get(0));
        populateDataCollection(dataCollectionNames().get(1));

        startConnector(Function.identity());

        waitForSnapshotToBeCompleted("mongodb", "mongo1", "0", null);

        List<Integer> expectedValues = IntStream.rangeClosed(0, 999).boxed().collect(Collectors.toList());

        assertRecordsWithValuesPresent(ROW_COUNT, expectedValues, topicName());

        stopConnector();
        assertConnectorNotRunning();

        List<String> collectionNames = List.of(FULL_COLLECTION_NAME, FULL_COLLECTION2_NAME);

        startConnector(x -> x.with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, String.join(",", collectionNames)));
        assertConnectorIsRunning();

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(
                Map.of(collectionNames.get(0), "{}",
                        collectionNames.get(1), "{error}"),
                collectionNames.get(0), collectionNames.get(1));

        waitForLogMessage("Snapshot was not completed successfully", AbstractSnapshotChangeEventSource.class);

        // Here we expect one record less since the last record (999) is buffered.
        // This to maintain the same behavior of initial snapshot.
        // Followup JIRA https://issues.redhat.com/browse/DBZ-8335
        expectedValues = IntStream.rangeClosed(0, 998).boxed().collect(Collectors.toList());

        waitForAvailableRecords(60, TimeUnit.SECONDS);

        assertRecordsWithValuesPresent(ROW_COUNT, expectedValues, topicName());

        insertRecords(1, ROW_COUNT * 2);

        waitForAvailableRecords();

        assertRecordsWithValuesPresent(1, List.of(2000), topicName(), consumeRecordsByTopic(1, 10));

        stopConnector();

        startConnector(Function.identity());

        assertNoRecordsToConsume();

    }

    protected Class<MongoDbConnector> connectorClass() {
        return MongoDbConnector.class;
    }

    public static int waitTimeForRecords() {
        return 3;
    }

    protected Configuration.Builder config() {
        return TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE_NAME)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, String.join(",", fullDataCollectionNames()))
                .with(MongoDbConnectorConfig.SIGNAL_DATA_COLLECTION, SIGNAL_COLLECTION_NAME)
                .with(MongoDbConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(MongoDbConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE_TABLES, "dbA.c1")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL);
    }

    protected String dataCollectionName() {
        return COLLECTION_NAME;
    }

    protected List<String> dataCollectionNames() {
        return List.of(COLLECTION_NAME, COLLECTION2_NAME);
    }

    protected String fullDataCollectionName() {
        return FULL_COLLECTION_NAME;
    }

    protected List<String> fullDataCollectionNames() {
        return List.of(FULL_COLLECTION_NAME, FULL_COLLECTION2_NAME);
    }

    protected String topicName() {
        return "mongo1" + "." + fullDataCollectionName();
    }

    protected List<String> topicNames() {
        return fullDataCollectionNames().stream().map(x -> "mongo1." + x).collect(Collectors.toList());
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

        final var rowsScanned = (TabularDataSupport) mbeanServer.getAttribute(getSnapshotMetricsObjectName(connector, server, task, database),
                "RowsScanned");

        final var scannedRowsByCollection = rowsScanned.values().stream().map(c -> ((CompositeDataSupport) c))
                .collect(Collectors.toMap(compositeDataSupport -> compositeDataSupport.get("key").toString(), compositeDataSupport -> compositeDataSupport.get("value")));

        final var unquotedCollectionName = table.replace("`", "");
        return (Long) scannedRowsByCollection.get(unquotedCollectionName);
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

        assertRecordsWithValuesPresent(expectedRecords, IntStream.range(2000, 2999).boxed().collect(Collectors.toList()), topicName());
    }

    private void assertRecordsFromSnapshotAndStreamingArePresent(int expectedRecords) throws InterruptedException {

        assertRecordsWithValuesPresent(expectedRecords, IntStream.range(0, expectedRecords - 2).boxed().collect(Collectors.toList()), topicName());
    }

    private void assertRecordsWithValuesPresent(int expectedRecords, List<Integer> expectedValues, String topicName) throws InterruptedException {

        SourceRecords snapshotAndStreamingRecords = consumeRecordsByTopic(expectedRecords, 10);
        assertRecordsWithValuesPresent(expectedRecords, expectedValues, topicName, snapshotAndStreamingRecords);
    }

    private void assertRecordsWithValuesPresent(int expectedRecords, List<Integer> expectedValues, String topicName, SourceRecords snapshotAndStreamingRecords)
            throws InterruptedException {

        assertThat(snapshotAndStreamingRecords.allRecordsInOrder().size()).isEqualTo(expectedRecords);
        List<Integer> actual = snapshotAndStreamingRecords.recordsForTopic(topicName).stream()
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

        final Document[] documents = new Document[rowCount];
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

    protected void sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(Map<String, String> additionalConditions, String... dataCollectionIds) {

        final String conditions = additionalConditions.entrySet().stream()
                .map(e -> String.format("{\"data-collection\": \"%s\", \"filter\": \"%s\"}", e.getKey(), e.getValue())).collect(
                        Collectors.joining(","));
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> "\"" + x + "\"")
                .collect(Collectors.joining(", "));
        insertDocuments("dbA", "signals",
                Document.parse("{\"type\": \"execute-snapshot\", \"payload\": {\"type\": \"BLOCKING\",\"data-collections\": [" + dataCollectionIdsList
                        + "], \"additional-conditions\": [" + conditions + "]}}"));
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        startConnector(custConfig, loggingCompletion());
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig, DebeziumEngine.CompletionCallback callback) {
        final Configuration config = custConfig.apply(config()).build();
        start(connectorClass(), config, callback);

        waitForAvailableRecords(5, TimeUnit.SECONDS);
    }
}
