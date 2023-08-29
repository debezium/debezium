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
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.TabularDataSupport;

import org.apache.kafka.connect.data.Struct;
import org.awaitility.Awaitility;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenConnectorUnderTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.AbstractSnapshotTest;
import io.debezium.util.Testing;

public abstract class AbstractBlockingSnapshotTest extends AbstractSnapshotTest {
    private int signalingRecords;

    protected static final int ROW_COUNT = 1000;

    protected abstract Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl);

    protected abstract JdbcConnection databaseConnection();

    @Override
    protected abstract String topicName();

    @Override
    protected abstract String tableName();

    @Override
    protected abstract String connector();

    @Override
    protected abstract String server();

    @Test
    public void executeBlockingSnapshot() throws Exception {
        // Testing.Print.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        insertRecords(ROW_COUNT, ROW_COUNT);

        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT * 2, consumeRecordsByTopic(ROW_COUNT * 2, 10));

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

        assertRecordsWithValuesPresent((int) ((ROW_COUNT * 3) + totalSnapshotRecords),
                getExpectedValues(totalSnapshotRecords), topicName(), consumeRecordsByTopic((int) ((ROW_COUNT * 3) + totalSnapshotRecords + signalingRecords), 10));
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

        assertRecordsWithValuesPresent(500, IntStream.rangeClosed(0, 499).boxed().collect(Collectors.toList()), topicNames().get(1).toString(),
                consumeRecordsByTopic(500 + signalingRecords, 10));

    }

    @Test
    @SkipWhenConnectorUnderTest(check = EqualityCheck.EQUAL, value = SkipWhenConnectorUnderTest.Connector.POSTGRES)
    @SkipWhenConnectorUnderTest(check = EqualityCheck.EQUAL, value = SkipWhenConnectorUnderTest.Connector.SQL_SERVER)
    @SkipWhenConnectorUnderTest(check = EqualityCheck.EQUAL, value = SkipWhenConnectorUnderTest.Connector.DB2)
    public void readsSchemaOnlyForSignaledTables() throws Exception {
        Testing.Print.enable();

        populateTable(tableNames().get(1).toString());

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(
                Map.of(tableDataCollectionIds().get(1), String.format("SELECT * FROM %s WHERE aa < 500", tableNames().get(1))), "", BLOCKING,
                tableDataCollectionIds().get(1).toString());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        signalingRecords = 1; // from streaming

        SourceRecords recordsByTopic = consumeRecordsByTopic(500 + signalingRecords + expectedDdlsCount(), 1);

        assertRecordsWithValuesPresent(500, IntStream.rangeClosed(0, 499).boxed().collect(Collectors.toList()), topicNames().get(1).toString(),
                recordsByTopic);

        List<String> ddls = recordsByTopic.recordsForTopic(server()).stream()
                .map(sourceRecord -> ((Struct) sourceRecord.value()).getString("ddl"))
                .collect(Collectors.toList());

        Testing.print(ddls);

        assertDdl(ddls);
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

    private void assertStreamingRecordsArePresent(int expectedRecords, SourceRecords recordsByTopic) throws InterruptedException {

        assertRecordsWithValuesPresent(expectedRecords, IntStream.range(2000, 2999).boxed().collect(Collectors.toList()), topicName(),
                recordsByTopic);
    }

    private void assertRecordsFromSnapshotAndStreamingArePresent(int expectedRecords, SourceRecords recordsByTopic) throws InterruptedException {

        assertRecordsWithValuesPresent(expectedRecords, IntStream.range(0, expectedRecords - 1).boxed().collect(Collectors.toList()), topicName(),
                recordsByTopic);
    }

    private void assertRecordsWithValuesPresent(int expectedRecords, List<Integer> expectedValues, String topicName, SourceRecords recordsByTopic) {

        List<Integer> actual = recordsByTopic.recordsForTopic(topicName).stream()
                .map(s -> ((Struct) s.value()).getStruct("after").getInt32(valueFieldName()))
                .collect(Collectors.toList());
        assertThat(recordsByTopic.recordsForTopic(topicName).size()).isEqualTo(expectedRecords);
        assertThat(actual).containsAll(expectedValues);
    }

    private void insertRecords(int rowCount, int startingPkId) throws SQLException {

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < rowCount; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName(),
                        connection.quotedColumnIdString(pkFieldName()),
                        i + startingPkId + 1,
                        i + startingPkId));
            }
            connection.commit();
        }
    }

    private void insertRecordsWithRandomSleep(int rowCount, int startingPkId, int maxSleep, Runnable actionOnInsert) throws SQLException {

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            for (int i = 0; i < rowCount; i++) {
                connection.execute(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName(),
                        connection.quotedColumnIdString(pkFieldName()),
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
