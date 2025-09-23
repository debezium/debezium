/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static io.debezium.pipeline.signal.actions.AbstractSnapshotSignal.SnapshotType.BLOCKING;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.TabularDataSupport;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.awaitility.Awaitility;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenConnectorUnderTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.AbstractSnapshotTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

public abstract class AbstractBlockingSnapshotTest<T extends SourceConnector> extends AbstractSnapshotTest<T> {
    private int signalingRecords;

    protected static final int ROW_COUNT = 1000;

    protected abstract Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl);

    protected abstract JdbcConnection databaseConnection();

    @Override
    protected abstract String topicName();

    @Override
    protected abstract String tableName();

    protected abstract String escapedTableDataCollectionId();

    @Override
    protected abstract String connector();

    @Override
    protected abstract String server();

    protected Configuration.Builder historizedMutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        return mutableConfig(signalTableOnly, storeOnlyCapturedDdl);
    }

    @Test
    public void executeBlockingSnapshot() throws Exception {
        // Testing.Print.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        insertRecords(ROW_COUNT, ROW_COUNT);

        SourceRecords consumedRecordsByTopic = consumeRecordsByTopic(ROW_COUNT * 2, 10);
        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT * 2, consumedRecordsByTopic);

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("", "", BLOCKING, tableDataCollectionId());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        signalingRecords = 1;

        assertRecordsFromSnapshotAndStreamingArePresent((ROW_COUNT * 2), consumeRecordsByTopic((ROW_COUNT * 2) + signalingRecords, 10));

        insertRecords(ROW_COUNT, ROW_COUNT * 2);

        assertStreamingRecordsArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT, 10));

    }

    @Test
    public void executeBlockingSnapshotWhileStreaming() throws Exception {
        // Testing.Debug.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        Future<?> batchInserts = executeAsync(insertTask());

        Thread.sleep(2000); // Let's start stream some insert

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("", "", BLOCKING, tableDataCollectionId());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        Long totalSnapshotRecords = getTotalSnapshotRecords(tableDataCollectionId(), connector(), server(), task(), database());

        batchInserts.get(120, TimeUnit.SECONDS);

        insertRecords(ROW_COUNT, (ROW_COUNT * 2));

        signalingRecords = 1; // from streaming

        SourceRecords consumeRecordsByTopic = consumeRecordsByTopic((int) ((ROW_COUNT * 3) + totalSnapshotRecords + signalingRecords), 10);
        assertRecordsWithValuesPresent((int) ((ROW_COUNT * 3) + totalSnapshotRecords),
                getExpectedValues(totalSnapshotRecords), topicName(), consumeRecordsByTopic);
    }

    @Test
    public void executeBlockingSnapshotWithAdditionalCondition() throws Exception {
        // Testing.Print.enable();

        populateTable(tableNames().get(1).toString());

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(
                Map.of(tableDataCollectionIds().get(1), String.format("SELECT * FROM %s WHERE aa < 500", tableNames().get(1))), "", BLOCKING,
                tableDataCollectionIds().get(1).toString());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        signalingRecords = 1; // from streaming

        SourceRecords consumedRecordsByTopic = consumeRecordsByTopic(500 + signalingRecords, 10);
        assertRecordsWithValuesPresent(500, IntStream.rangeClosed(0, 499).boxed().collect(Collectors.toList()), topicNames().get(1).toString(),
                consumedRecordsByTopic);

    }

    @Test
    @FixFor("DBZ-8238")
    public void streamingMetricsResumeAfterBlockingSnapshot() throws Exception {
        // Testing.Print.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(
                Map.of(tableName(), String.format("SELECT * FROM %s WHERE aa < 500", tableName())), "", BLOCKING,
                tableDataCollectionId());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        insertRecords(ROW_COUNT, (ROW_COUNT * 2));

        signalingRecords = 1;
        Long expectedTotalStreamingCreateEvents = (long) (ROW_COUNT + signalingRecords);

        assertStreamingTotalNumberOfCreateEventsSeen(expectedTotalStreamingCreateEvents);
    }

    @Test
    @SkipWhenConnectorUnderTest(check = EqualityCheck.EQUAL, value = SkipWhenConnectorUnderTest.Connector.POSTGRES)
    @SkipWhenConnectorUnderTest(check = EqualityCheck.EQUAL, value = SkipWhenConnectorUnderTest.Connector.SQL_SERVER)
    @SkipWhenConnectorUnderTest(check = EqualityCheck.EQUAL, value = SkipWhenConnectorUnderTest.Connector.DB2)
    public void readsSchemaOnlyForSignaledTables() throws Exception {
        // Testing.Print.enable();

        populateTable(tableNames().get(1));

        startConnectorWithSnapshot(x -> historizedMutableConfig(false, false));

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(
                Map.of(tableDataCollectionIds().get(1), String.format("SELECT * FROM %s WHERE aa < 500", tableNames().get(1))), "", BLOCKING,
                tableDataCollectionIds().get(1));

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        signalingRecords = 1; // from streaming

        SourceRecords recordsByTopic = consumeRecordsByTopic(500 + signalingRecords + expectedDdlsCount(), 1);

        assertRecordsWithValuesPresent(500, IntStream.rangeClosed(0, 499).boxed().collect(Collectors.toList()), topicNames().get(1).toString(),
                recordsByTopic);

        List<String> ddls = recordsByTopic.recordsForTopic(server()).stream()
                .map(sourceRecord -> ((Struct) sourceRecord.value()).getString("ddl"))
                .collect(Collectors.toList());

        assertDdl(ddls);
    }

    @Test
    @FixFor("DBZ-7718")
    public void executeBlockingSnapshotWithEscapedCollectionName() throws Exception {
        // Testing.Print.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        insertRecords(ROW_COUNT, ROW_COUNT);

        SourceRecords consumedRecordsByTopic = consumeRecordsByTopic(ROW_COUNT * 2, 10);
        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT * 2, consumedRecordsByTopic);

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("", "", BLOCKING, escapedTableDataCollectionId());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        signalingRecords = 1;

        assertRecordsFromSnapshotAndStreamingArePresent((ROW_COUNT * 2), consumeRecordsByTopic((ROW_COUNT * 2) + signalingRecords, 10));

        insertRecords(ROW_COUNT, ROW_COUNT * 2);

        assertStreamingRecordsArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT, 10));

    }

    @Test
    @FixFor("DBZ-8244")
    public void anErrorDuringBlockingSnapshotShouldLeaveTheConnectorInAGoodState() throws Exception {
        // Testing.Print.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false)
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 1));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        insertRecords(ROW_COUNT, ROW_COUNT);

        SourceRecords consumedRecordsByTopic = consumeRecordsByTopic(ROW_COUNT * 2, 20);
        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT * 2, consumedRecordsByTopic);

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(
                Map.of(tableDataCollectionIds().get(1), "SELECT WITH AN ERROR"), "", BLOCKING,
                tableDataCollectionIds().get(1));

        waitForLogMessage("Snapshot was not completed successfully", AbstractSnapshotChangeEventSource.class);

        insertRecords(ROW_COUNT, ROW_COUNT * 2);

        signalingRecords = 1;

        assertStreamingRecordsArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT + signalingRecords, 10));

    }

    @FixFor("DBZ-7903")
    @Test
    public void aFailedBlockingSnapshotShouldNotCauseInitialSnapshotOnRestart() throws Exception {
        // Testing.Print.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        SourceRecords consumedRecordsByTopic = consumeRecordsByTopic(ROW_COUNT, 10);
        List<Integer> expectedValues = IntStream.rangeClosed(0, 999).boxed().collect(Collectors.toList());

        assertRecordsWithValuesPresent(ROW_COUNT, expectedValues, topicName(), consumedRecordsByTopic);

        stopConnector();
        assertConnectorNotRunning();

        startConnectorWithSnapshot(x -> mutableConfig(false, false)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, String.join(",", tableDataCollectionIds())));

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(
                Map.of(tableDataCollectionIds().get(0), String.format("SELECT * FROM %s ORDER BY PK ASC", tableNames().get(0)),
                        tableDataCollectionIds().get(1), "SELECT failing query"),
                "", BLOCKING,
                tableDataCollectionIds().get(0), tableDataCollectionIds().get(1));

        waitForLogMessage("Snapshot was not completed successfully", AbstractSnapshotChangeEventSource.class);

        // Here we expect one record less since the last record (999) is buffered.
        // This to maintain the same behavior of initial snapshot.
        // Followup JIRA https://issues.redhat.com/browse/DBZ-8335
        consumedRecordsByTopic = consumeRecordsByTopic(ROW_COUNT, 10);
        expectedValues = IntStream.rangeClosed(0, 998).boxed().collect(Collectors.toList());

        assertRecordsWithValuesPresent(ROW_COUNT - 1, expectedValues, topicName(), consumedRecordsByTopic);

        insertRecords(1, ROW_COUNT * 2);

        waitForAvailableRecords();

        assertRecordsWithValuesPresent(1, List.of(2000), topicName(), consumeRecordsByTopic(1, 10));

        stopConnector();

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        assertNoRecordsToConsume();

    }

    @FixFor("DBZ-9337")
    @Test
    public void blockingSnapshotMustReuseExistingOffsetAsSnapshotOffset() throws Exception {

        // Testing.Print.enable();

        populateTable();
        insertRecords(200, 0, tableNames().get(1));

        startConnectorWithSnapshot(x -> mutableConfig(false, false)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "[A-z].*a")
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "[A-z].*a")
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 2)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, 3)
                .with(EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS, 0));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        SourceRecords consumedRecordsByTopic = consumeRecordsByTopic(ROW_COUNT, 10);
        List<Integer> expectedValues = IntStream.rangeClosed(0, 999).boxed().collect(Collectors.toList());

        assertRecordsWithValuesPresent(ROW_COUNT, expectedValues, topicName(), consumedRecordsByTopic);

        stopConnector();
        assertConnectorNotRunning();

        insertRecords(1000, ROW_COUNT, tableNames().get(0));
        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("", "", BLOCKING, "[A-z].*b");
        insertRecords(1000, ROW_COUNT + 1000, tableNames().get(0));

        start(connectorClass(), mutableConfig(false, false)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "[A-z].*[ab]")
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 2)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, 3)
                .with(EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS, 0)
                .build(), null, (record) -> {

                    if (record.key() != null) {
                        Testing.print("Consuming record with key {" + ((Struct) record.key()).get(pkFieldName()) + "} from topic {" + record.topic() + "} "
                                + ((Struct) record.value()).get("source"));
                    }
                    else {
                        Testing.print("Consuming record with key {} from topic {" + record.topic() + "} " + ((Struct) record.value()).get("source"));
                    }
                    if (record.topic().equals(topicNames().get(1))) {
                        Struct key = (Struct) record.key();
                        Number id = (Number) key.get(pkFieldName());
                        return id.intValue() == 100;
                    }

                    return false;
                });

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> !isEngineRunning.get());

        try {
            stopConnector();
        }
        catch (IllegalStateException e) {
            // ignoring since it is already stopped.
        }

        startConnectorWithSnapshot(x -> mutableConfig(false, false)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "[A-z].*[ab]")
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 2)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, 3)
                .with(EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS, 0));

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        insertRecords(1, ROW_COUNT + 2000, tableName());

        waitForAvailableRecords();

        assertRecordsWithValuesPresent(2001, IntStream.rangeClosed(1000, 2000).boxed().collect(Collectors.toList()), topicName(), consumeRecordsByTopic(2101, 20));

        stopConnector();
    }

    @Test
    @FixFor("DBZ-9494")
    public void anErrorDuringBlockingSnapshotShouldNotLeaveTheStreamingPaused() throws Exception {
        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false)
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 1));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        insertRecords(ROW_COUNT, ROW_COUNT);

        SourceRecords consumedRecordsByTopic = consumeRecordsByTopic(ROW_COUNT * 2, 20);
        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT * 2, consumedRecordsByTopic);

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(
                String.format("{\"data-collection\": \"%s\"}", tableDataCollectionIds().get(1)), "", BLOCKING,
                tableDataCollectionIds().get(1));

        waitForLogMessage("Error while executing requested blocking snapshot.", ChangeEventSourceCoordinator.class);

        insertRecords(ROW_COUNT, ROW_COUNT * 2);

        signalingRecords = 1;

        assertStreamingRecordsArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT + signalingRecords, 10));

    }

    protected int expectedDdlsCount() {
        return 0;
    };

    protected void assertDdl(List<String> schemaChangesDdls) {
    };

    protected int insertMaxSleep() {
        return 2;
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

    private Long getTotalStreamingCreateEventsSeen(String connector, String server, String task, String database) throws MalformedObjectNameException,
            ReflectionException, AttributeNotFoundException, InstanceNotFoundException,
            MBeanException {

        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        ObjectName objectName = getStreamingMetricsObjectName(connector, server, "streaming", task, database);

        return (Long) mbeanServer.getAttribute(objectName, "TotalNumberOfCreateEventsSeen");
    }

    private Long getTotalSnapshotRecords(String table, String connector, String server, String task, String database) throws MalformedObjectNameException,
            ReflectionException, AttributeNotFoundException, InstanceNotFoundException,
            MBeanException {

        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        TabularDataSupport rowsScanned = (TabularDataSupport) mbeanServer.getAttribute(getSnapshotMetricsObjectName(connector, server, task, database),
                "RowsScanned");

        Map<String, Object> scannedRowsByTable = rowsScanned.values().stream().map(c -> ((CompositeDataSupport) c))
                .collect(Collectors.toMap(compositeDataSupport -> compositeDataSupport.get("key").toString(), compositeDataSupport -> compositeDataSupport.get("value")));

        String unquotedTableName = table.replace("`", "");
        return (Long) scannedRowsByTable.get(unquotedTableName);
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

    protected static void waitForLogMessage(String message, Class<?> logEmitterClass) {
        LogInterceptor interceptor = new LogInterceptor(logEmitterClass);
        Awaitility.await()
                .alias("Snapshot not completed on time")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 60L, TimeUnit.SECONDS)
                .until(() -> interceptor.containsMessage(message));
    }

    private void assertStreamingTotalNumberOfCreateEventsSeen(Long expectedStreamingEvents) throws ReflectionException,
            MalformedObjectNameException, AttributeNotFoundException, InstanceNotFoundException, MBeanException {
        try {
            Awaitility.await()
                    .pollInterval(1000L, TimeUnit.MILLISECONDS)
                    .atMost(waitTimeForRecords() * 30L, TimeUnit.SECONDS)
                    .until(() -> Objects.equals(getTotalStreamingCreateEventsSeen(connector(), server(), task(), database()), expectedStreamingEvents));
        }
        catch (org.awaitility.core.ConditionTimeoutException ignored) {

        }
        finally {
            Long actualStreamingEvents = getTotalStreamingCreateEventsSeen(connector(), server(), task(), database());
            assertThat(actualStreamingEvents)
                    .withFailMessage("streaming TotalNumberOfCreateEventsSeen metric value expected: %d actual: %d", expectedStreamingEvents, actualStreamingEvents)
                    .isEqualTo(expectedStreamingEvents);
        }
    }

    private Future<?> executeAsync(Runnable operation) {
        return Executors.newSingleThreadExecutor().submit(operation);
    }

    protected void assertStreamingRecordsArePresent(int expectedRecords, SourceRecords recordsByTopic) {

        assertRecordsWithValuesPresent(expectedRecords, IntStream.rangeClosed(2000, 2999).boxed().collect(Collectors.toList()), topicName(),
                recordsByTopic);
    }

    protected void assertRecordsFromSnapshotAndStreamingArePresent(int expectedRecords, SourceRecords recordsByTopic) throws InterruptedException {

        assertRecordsWithValuesPresent(expectedRecords, IntStream.range(0, expectedRecords - 1).boxed().collect(Collectors.toList()), topicName(),
                recordsByTopic);
    }

    protected void assertRecordsWithValuesPresent(int expectedRecords, List<Integer> expectedValues, String topicName, SourceRecords recordsByTopic) {

        List<Integer> actual = recordsByTopic.recordsForTopic(topicName).stream()
                .map(s -> ((Struct) s.value()).getStruct("after").getInt32(valueFieldName()))
                .collect(Collectors.toList());
        assertThat(recordsByTopic.recordsForTopic(topicName).size()).isEqualTo(expectedRecords);
        assertThat(actual).containsAll(expectedValues);
    }

    protected void insertRecords(int rowCount, int startingPkId, String tableName) throws SQLException {

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < rowCount; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName,
                        connection.quoteIdentifier(pkFieldName()),
                        i + startingPkId + 1,
                        i + startingPkId));
            }
            connection.commit();
        }
    }

    protected void insertRecords(int rowCount, int startingPkId) throws SQLException {

        insertRecords(rowCount, startingPkId, tableName());
    }

    private void insertRecordsWithRandomSleep(int rowCount, int startingPkId, int maxSleep, Runnable actionOnInsert) throws SQLException {

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            for (int i = 0; i < rowCount; i++) {
                connection.execute(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName(),
                        connection.quoteIdentifier(pkFieldName()),
                        i + startingPkId + 1,
                        i + startingPkId));
                actionOnInsert.run();
                int sleepTime = ThreadLocalRandom.current().nextInt(1, maxSleep);
                Thread.sleep(sleepTime);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void insertRecordsWithRandomSleep(int rowCount, int startingPkId, int maxSleep) throws SQLException {

        insertRecordsWithRandomSleep(rowCount, startingPkId, maxSleep, () -> {
        });
    }
}
