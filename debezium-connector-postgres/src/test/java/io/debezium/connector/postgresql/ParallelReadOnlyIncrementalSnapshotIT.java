/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.notification.IncrementalSnapshotNotificationService;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.ParallelIncrementalSnapshotCoordinator;
import io.debezium.pipeline.source.snapshot.incremental.ParallelIncrementalSnapshotReadStrategy;

/**
 * Runs the read-only incremental snapshot suite with {@code snapshot.max.threads} above one, so every
 * inherited scenario (signals, stop, pause and resume, restarts, additional conditions, notifications)
 * goes through {@link ParallelIncrementalSnapshotReadStrategy}, and adds the multi-table cases the
 * parallel path exists for.
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 13, minor = 0, reason = "Function pg_current_snapshot() not supported until PostgreSQL 13")
public class ParallelReadOnlyIncrementalSnapshotIT extends ReadOnlyIncrementalSnapshotIT {

    private static final String NOTIFICATION_TOPIC = "io.debezium.notification";

    // One more than the two tables most scenarios snapshot, so a worker slot stays idle and both the
    // "fewer tables than threads" and the "all slots busy" paths of the coordinator are exercised.
    private static final int SNAPSHOT_THREADS = 3;

    @Override
    protected Configuration.Builder config() {
        return super.config().with(PostgresConnectorConfig.SNAPSHOT_MAX_THREADS, SNAPSHOT_THREADS);
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        return super.mutableConfig(signalTableOnly, storeOnlyCapturedDdl)
                .with(PostgresConnectorConfig.SNAPSHOT_MAX_THREADS, SNAPSHOT_THREADS);
    }

    @Test
    @FixFor("dbz#1829")
    public void shouldSnapshotMultipleTablesInParallel() throws Exception {
        final LogInterceptor sourceLog = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);
        final LogInterceptor strategyLog = new LogInterceptor(ParallelIncrementalSnapshotReadStrategy.class);
        final LogInterceptor coordinatorLog = new LogInterceptor(ParallelIncrementalSnapshotCoordinator.class);

        populateTables();
        startConnector();
        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        sendAdHocSnapshotSignal(tableDataCollectionIds().toArray(new String[0]));

        final int expectedRecordCount = ROW_COUNT * tableNames().size();
        final AtomicInteger recordCounter = new AtomicInteger();
        final List<SourceRecord> dbChanges = new ArrayList<>();
        consumeRecordsUntil((consumed, record) -> recordCounter.get() == expectedRecordCount,
                (consumed, record) -> "",
                5,
                record -> {
                    if (topicNames().contains(record.topic())) {
                        dbChanges.add(record);
                        recordCounter.incrementAndGet();
                    }
                },
                false);

        final Map<String, Map<Integer, Integer>> rowsByTopic = dbChanges.stream()
                .collect(Collectors.groupingBy(SourceRecord::topic,
                        Collectors.toMap(record -> ((Struct) record.key()).getInt32(pkFieldName()),
                                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()))));
        for (String topic : topicNames()) {
            final Map<Integer, Integer> rows = rowsByTopic.get(topic);
            assertThat(rows).as(topic).isNotNull().hasSize(ROW_COUNT);
            for (int i = 0; i < ROW_COUNT; i++) {
                assertThat(rows).as(topic).contains(entry(i + 1, i));
            }
        }

        // The rows must have been read by the parallel strategy, not by the sequential fallback.
        assertThat(sourceLog.containsMessage("Incremental snapshot multi-threading enabled with " + SNAPSHOT_THREADS + " threads")).isTrue();
        assertThat(strategyLog.containsMessage(
                "Starting PARALLEL incremental snapshot for " + tableNames().size() + " tables with " + SNAPSHOT_THREADS + " worker threads")).isTrue();
        assertThat(coordinatorLog.containsMessage("Parallel snapshot initialized: " + tableNames().size() + " active, 0 pending")).isTrue();
        for (String table : tableNames()) {
            assertThat(strategyLog.containsMessage("Parallel snapshot of table '" + table + "' completed")).as(table).isTrue();
        }
        assertThat(strategyLog.containsMessage("reading on the streaming connection")).isFalse();
    }

    @Test
    @FixFor("dbz#1829")
    public void shouldReportCompletionOfParallelSnapshot() throws Exception {
        populateTables();
        startConnector(x -> x.with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink")
                .with(SinkNotificationChannel.NOTIFICATION_TOPIC, NOTIFICATION_TOPIC), loggingCompletion(), false);
        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        sendAdHocSnapshotSignal(tableDataCollectionIds().toArray(new String[0]));

        final SourceRecords records = consumeRecordsByTopicUntil((consumed, record) -> isIncrementalSnapshotNotification(record, "COMPLETED"));

        final List<Struct> notifications = records.recordsForTopic(NOTIFICATION_TOPIC).stream()
                .map(record -> (Struct) record.value())
                .filter(notification -> IncrementalSnapshotNotificationService.INCREMENTAL_SNAPSHOT.equals(notification.getString("aggregate_type")))
                .toList();
        final List<String> types = notifications.stream().map(notification -> notification.getString("type")).toList();

        assertThat(types).as("notification types").contains("STARTED", "IN_PROGRESS", "TABLE_SCAN_COMPLETED", "COMPLETED");
        assertThat(types).as("the snapshot completes once").filteredOn("COMPLETED"::equals).hasSize(1);

        // Every table reports its own scan, and the completion closes the snapshot started by the signal.
        final List<String> scannedCollections = notifications.stream()
                .filter(notification -> "TABLE_SCAN_COMPLETED".equals(notification.getString("type")))
                .map(notification -> notification.getMap("additional_data").get(IncrementalSnapshotNotificationService.SCANNED_COLLECTION))
                .map(Object::toString)
                .toList();
        assertThat(scannedCollections).containsExactlyInAnyOrderElementsOf(tableDataCollectionIds());
        assertThat(notifications.stream().map(notification -> notification.getString("id")).collect(Collectors.toSet())).containsExactly("ad-hoc");
        assertThat(notifications.stream().filter(notification -> "IN_PROGRESS".equals(notification.getString("type")))
                .map(notification -> notification.getMap("additional_data").get(IncrementalSnapshotNotificationService.CURRENT_COLLECTION_IN_PROGRESS))
                .map(Object::toString)
                .collect(Collectors.toSet()))
                .as("progress is reported per table")
                .containsExactlyInAnyOrderElementsOf(tableDataCollectionIds());
    }

    private static boolean isIncrementalSnapshotNotification(SourceRecord record, String type) {
        if (!NOTIFICATION_TOPIC.equals(record.topic())) {
            return false;
        }
        final Struct value = (Struct) record.value();
        return IncrementalSnapshotNotificationService.INCREMENTAL_SNAPSHOT.equals(value.getString("aggregate_type"))
                && type.equals(value.getString("type"));
    }
}
