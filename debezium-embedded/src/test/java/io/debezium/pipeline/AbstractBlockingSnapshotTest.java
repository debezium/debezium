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
import java.util.Optional;
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
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.AbstractSnapshotTest;

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

        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT * 2);

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional.empty(), Optional.empty(), BLOCKING, tableDataCollectionId());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        signalingRecords = 1;
        assertRecordsFromSnapshotAndStreamingArePresent((ROW_COUNT * 2) + signalingRecords);

        insertRecords(ROW_COUNT, (ROW_COUNT * 2));

        assertStreamingRecordsArePresent(ROW_COUNT + signalingRecords);

    }

    @Test
    public void executeBlockingSnapshotWhileStreaming() throws Exception {
        // Testing.Debug.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        Future<?> batchInserts = executeAsync(insertTask());

        Thread.sleep(2000); // Let's start stream some insert

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional.empty(), Optional.empty(), BLOCKING, tableDataCollectionId());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        Long totalSnapshotRecords = getTotalSnapshotRecords(tableDataCollectionId(), connector(), server(), task(), database());

        batchInserts.get(120, TimeUnit.SECONDS);

        insertRecords(ROW_COUNT, (ROW_COUNT * 2));

        signalingRecords = 1 + // from streaming
                1; // from snapshot

        assertRecordsWithValuesPresent((int) ((ROW_COUNT * 3) + totalSnapshotRecords + signalingRecords),
                getExpectedValues(totalSnapshotRecords));
    }

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

    private void assertStreamingRecordsArePresent(int expectedRecords) throws InterruptedException {

        assertRecordsWithValuesPresent(expectedRecords, IntStream.range(2000, 2999).boxed().collect(Collectors.toList()));
    }

    private void assertRecordsFromSnapshotAndStreamingArePresent(int expectedRecords) throws InterruptedException {

        assertRecordsWithValuesPresent(expectedRecords, IntStream.range(0, expectedRecords - 1).boxed().collect(Collectors.toList()));
    }

    private void assertRecordsWithValuesPresent(int expectedRecords, List<Integer> expectedValues) throws InterruptedException {

        SourceRecords snapshotAndStreamingRecords = consumeRecordsByTopic(expectedRecords, 10);
        assertThat(snapshotAndStreamingRecords.allRecordsInOrder().size()).isEqualTo(expectedRecords);
        List<Integer> actual = snapshotAndStreamingRecords.recordsForTopic(topicName()).stream()
                .map(s -> ((Struct) s.value()).getStruct("after").getInt32(valueFieldName()))
                .collect(Collectors.toList());
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
