/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoQueryException;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.SnapshotMode;
import io.debezium.connector.mongodb.sink.MongoDbSinkConnectorConfig;
import io.debezium.connector.mongodb.sink.MongoDbSinkConnectorTask;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.source.kafka.KafkaConnectSourceTaskContextAdapter;
import io.debezium.storage.kafka.offset.KafkaMemoryOffsetProvider;
import io.debezium.testing.testcontainers.MongoDbContainer;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;

class MongoDbConnectionLifecycleIT extends AbstractMongoConnectorIT {
    private Configuration configuration(ConnectionResourceTracker tracker) {
        return tracker.track(TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "lifecycle.items,lifecycle.other")
                .with(MongoDbConnectorConfig.SIGNAL_DATA_COLLECTION, "lifecycle.signals")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "lifecycle_sink")
                .build());
    }

    @Test
    void shouldReleaseSourceValidationResources() {
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker);
            for (int attempt = 0; attempt < 3; attempt++) {
                final var result = new MongoDbConnector().validate(config.asMap());
                assertThat(result.configValues()).allSatisfy(value -> assertThat(value.errorMessages()).isEmpty());
                tracker.assertClientsCreated();
                tracker.assertReleased();
            }
        }
    }

    @Test
    void shouldReleaseSinkValidationResources() {
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker);
            for (int attempt = 0; attempt < 10; attempt++) {
                final var result = new MongoDbSinkConnector().validate(config.asMap());
                assertThat(result.configValues()).allSatisfy(value -> assertThat(value.errorMessages()).isEmpty());
                tracker.assertClientsCreated();
                tracker.assertReleased();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldReportValidationInitializationFailureAndReleaseResources(boolean source) {
        try (var tracker = new ConnectionResourceTracker()) {
            tracker.settingsFailure = new IllegalStateException("Authentication settings failed");
            final var config = configuration(tracker);
            final var validation = source ? new MongoDbConnector().validate(config.asMap()) : new MongoDbSinkConnector().validate(config.asMap());
            assertThat(validation.configValues()).anySatisfy(value -> assertThat(value.errorMessages())
                    .anySatisfy(message -> assertThat(message).contains(tracker.settingsFailure.getMessage())));
            tracker.assertReleased();
        }
    }

    @Test
    void shouldReleaseResourcesAfterValidationTimeout() {
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker).edit()
                    .with(MongoDbConnectorConfig.CONNECTION_STRING, "mongodb://127.0.0.1:1/")
                    .with(CommonConnectorConfig.CONNECTION_VALIDATION_TIMEOUT_MS, 300)
                    .with(MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS, 10_000)
                    .build();
            final var result = new ConfigValue(MongoDbConnectorConfig.CONNECTION_STRING.name());
            new MongoDbConnector().validateConnection(config, result);
            assertThat(result.errorMessages()).anySatisfy(message -> assertThat(message).contains("timed out"));
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(tracker::assertReleased);
            tracker.assertClientsCreated();
        }
    }

    @ParameterizedTest
    @EnumSource(value = SnapshotMode.class, names = { "INITIAL", "NO_DATA" })
    void shouldReleaseSourceResourcesAfterSnapshotAndStreaming(SnapshotMode mode) throws Exception {
        insertDocuments("lifecycle", "items", new Document("_id", 1));
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker).edit().with(MongoDbConnectorConfig.SNAPSHOT_MODE, mode).build();
            try {
                start(MongoDbConnector.class, config);
                waitForStreamingRunning("mongodb", "mongo1");
                insertDocuments("lifecycle", "items", new Document("_id", 2));
                final int expectedRecords = mode == SnapshotMode.INITIAL ? 2 : 1;
                assertThat(consumeRecordsByTopic(expectedRecords).recordsForTopic("mongo1.lifecycle.items")).hasSize(expectedRecords);
            }
            finally {
                stopConnector();
            }
            tracker.assertClientsCreated();
            tracker.assertReleased();
        }
    }

    @Test
    void shouldReleaseResourcesAfterGuardrailFailureAndAllowRestart() throws Exception {
        insertDocuments("lifecycle", "items", new Document("_id", 1));
        insertDocuments("lifecycle", "other", new Document("_id", 1));
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker).edit()
                    .with(CommonConnectorConfig.GUARDRAIL_COLLECTIONS_MAX, 1)
                    .with(CommonConnectorConfig.GUARDRAIL_COLLECTIONS_LIMIT_ACTION, "fail")
                    .build();
            final var failed = new CompletableFuture<Throwable>();
            start(MongoDbConnector.class, config, (success, message, error) -> failed.complete(error));
            assertThat(failed.get(30, TimeUnit.SECONDS)).isNotNull();
            stopConnector();
            tracker.assertClientsCreated();
            tracker.assertReleased();
            try {
                start(MongoDbConnector.class, config.edit().with(CommonConnectorConfig.GUARDRAIL_COLLECTIONS_MAX, 2).build());
                waitForStreamingRunning("mongodb", "mongo1");
            }
            finally {
                stopConnector();
            }
            tracker.assertReleased();
        }
    }

    @Test
    void shouldReleaseResourcesAfterRepeatedSourceRestarts() throws Exception {
        insertDocuments("lifecycle", "items", new Document("_id", 1));
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker).edit().with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA).build();
            for (int attempt = 0; attempt < 5; attempt++) {
                try {
                    start(MongoDbConnector.class, config);
                    waitForStreamingRunning("mongodb", "mongo1");
                    insertDocuments("lifecycle", "items", new Document("_id", attempt + 2));
                    consumeRecordsByTopic(1);
                }
                finally {
                    stopConnector();
                }
                tracker.assertClientsCreated();
                tracker.assertReleased();
            }
        }
    }

    @Test
    void shouldReleaseSinkResourcesAfterRepeatedStops() {
        try (var tracker = new ConnectionResourceTracker()) {
            for (int attempt = 0; attempt < 5; attempt++) {
                final var task = new MongoDbSinkConnectorTask();
                try {
                    task.start(configuration(tracker).asMap());
                    task.put(List.of());
                }
                finally {
                    task.stop();
                    task.stop();
                }
                tracker.assertClientsCreated();
                tracker.assertReleased();
            }
        }
    }

    @Test
    void shouldAllowAnotherIncrementalSnapshotBeforeReleasingResources() throws Exception {
        insertDocuments("lifecycle", "items", new Document("_id", 1));
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker).edit()
                    .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(MongoDbConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 10)
                    .with(MongoDbConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                    .build();
            try {
                start(MongoDbConnector.class, config);
                waitForStreamingRunning("mongodb", "mongo1");
                for (int attempt = 0; attempt < 2; attempt++) {
                    insertDocuments("lifecycle", "signals", new Document("type", "execute-snapshot")
                            .append("payload", new Document("data-collections", List.of("lifecycle.items"))));
                    final List<SourceRecord> records = new ArrayList<>();
                    Awaitility.await().pollInSameThread().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                        consumeAvailableRecords(record -> {
                            if (record.topic().equals("mongo1.lifecycle.items")) {
                                records.add(record);
                            }
                        });
                        assertThat(records).hasSize(1);
                    });
                    assertThat(((Struct) records.get(0).value()).getString("op")).isEqualTo("r");
                    tracker.assertSnapshotWorkersCreated();
                }
            }
            finally {
                stopConnector();
            }
            tracker.assertClientsCreated();
            tracker.assertReleased();
        }
    }

    @Test
    void shouldKeepOtherSinkTaskWritingAfterOneTaskStops() {
        try (var first = new ConnectionResourceTracker(); var second = new ConnectionResourceTracker()) {
            final var firstTask = new MongoDbSinkConnectorTask();
            final var secondTask = new MongoDbSinkConnectorTask();
            try {
                firstTask.start(configuration(first).asMap());
                secondTask.start(configuration(second).asMap());
                firstTask.put(List.of(sinkRecord(1)));
                firstTask.stop();
                first.assertReleased();
                secondTask.put(List.of(sinkRecord(2)));
                second.assertAuthenticationActive();
                try (var client = TestHelper.connect(mongo)) {
                    assertThat(client.getDatabase("lifecycle_sink").getCollection("lifecycle_items").countDocuments()).isEqualTo(2);
                }
            }
            finally {
                firstTask.stop();
                secondTask.stop();
            }
            second.assertClientsCreated();
            second.assertReleased();
        }
    }

    private SinkRecord sinkRecord(int id) {
        final var rowSchema = SchemaBuilder.struct().name("lifecycle.Item").field("id", Schema.INT32_SCHEMA).build();
        final var sourceSchema = SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
        final var envelopeSchema = SchemaBuilder.struct().name("lifecycle.Item.Envelope")
                .field("after", rowSchema).field("source", sourceSchema).field("op", Schema.STRING_SCHEMA).build();
        final var value = new Struct(envelopeSchema)
                .put("after", new Struct(rowSchema).put("id", id))
                .put("source", new Struct(sourceSchema).put("ts_ms", 1L)).put("op", "c");
        return new SinkRecord("lifecycle.items", 0, rowSchema, new Struct(rowSchema).put("id", id), envelopeSchema, value, id);
    }

    @Test
    void shouldReleaseSinkResourcesWhenInitializationFails() {
        try (var tracker = new ConnectionResourceTracker()) {
            tracker.settingsFailure = new IllegalStateException("Cannot configure authentication");
            final var task = new MongoDbSinkConnectorTask();
            assertThatThrownBy(() -> task.start(configuration(tracker).asMap()))
                    .hasRootCause(tracker.settingsFailure);
            task.stop();
            tracker.assertReleased();
        }
    }

    @Test
    void shouldReleaseSinkResourcesWhenStartupFailsAfterClientCreation() {
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker).edit()
                    .with("name", "lifecycle-sink-startup-failure")
                    .with("openlineage.integration.enabled", true)
                    .with("openlineage.integration.job.tags", "invalid-tag-without-value")
                    .build();
            final var task = new MongoDbSinkConnectorTask();
            assertThatThrownBy(() -> task.start(config.asMap()))
                    .isInstanceOf(ConnectException.class)
                    .hasRootCauseInstanceOf(ArrayIndexOutOfBoundsException.class);
            // Verify cleanup without relying on Kafka Connect to call stop() after start() fails.
            tracker.assertReleased();
            tracker.assertClientsCreated();
        }
    }

    @Test
    void shouldReleaseSinkValidationResourcesOnTimeout() {
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker).edit()
                    .with(MongoDbConnectorConfig.CONNECTION_STRING, "mongodb://127.0.0.1:1/?connectTimeoutMS=100")
                    .build();
            final var result = new MongoDbSinkConnector().validate(config.asMap());
            assertThat(result.configValues()).anySatisfy(value -> assertThat(value.errorMessages()).contains("Unable to connect to the server."));
            tracker.assertReleased();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldReleaseValidationResourcesOnInterrupt(boolean source) throws Exception {
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = configuration(tracker).edit()
                    .with(MongoDbConnectorConfig.CONNECTION_STRING, "mongodb://127.0.0.1:1/?connectTimeoutMS=30000")
                    .build();
            final var result = new CompletableFuture<Boolean>();
            final var worker = new Thread(() -> {
                try {
                    if (source) {
                        final var validation = new MongoDbConnector().validate(config.asMap());
                        assertThat(validation.configValues()).anySatisfy(value -> assertThat(value.errorMessages()).contains("Connection validation interrupted"));
                    }
                    else {
                        new MongoDbSinkConnector().validate(config.asMap());
                    }
                    result.complete(Thread.currentThread().isInterrupted());
                }
                catch (ConnectException e) {
                    result.complete(Thread.currentThread().isInterrupted());
                }
                catch (Throwable e) {
                    result.completeExceptionally(e);
                }
            });
            try {
                worker.start();
                Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(tracker::assertClientsCreated);
                worker.interrupt();
                assertThat(result.get(10, TimeUnit.SECONDS)).isTrue();
                Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(tracker::assertReleased);
            }
            finally {
                worker.interrupt();
                worker.join(10_000);
                assertThat(worker.isAlive()).isFalse();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = SnapshotMode.class, names = { "INITIAL", "NO_DATA" })
    void shouldReleaseResourcesWhenSnapshotReadIsInterrupted(SnapshotMode mode) throws Exception {
        insertDocuments("lifecycle", "items", new Document("_id", 1));
        final var reading = new CountDownLatch(1);
        final var releaseRead = new CountDownLatch(1);
        try (var tracker = new ConnectionResourceTracker()) {
            tracker.commandListener = blockSnapshotRead(reading, releaseRead, mode == SnapshotMode.NO_DATA);
            final var config = configuration(tracker).edit()
                    .with(MongoDbConnectorConfig.SNAPSHOT_MODE, mode)
                    .with(MongoDbConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                    .with(CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_MS, 500)
                    .build();
            try {
                start(MongoDbConnector.class, config);
                if (mode == SnapshotMode.NO_DATA) {
                    waitForStreamingRunning("mongodb", "mongo1");
                    insertDocuments("lifecycle", "signals", new Document("type", "execute-snapshot")
                            .append("payload", new Document("data-collections", List.of("lifecycle.items"))));
                }
                assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();
                stopConnector();
                tracker.assertClientsCreated();
                tracker.assertReleased();
            }
            finally {
                releaseRead.countDown();
                stopConnector();
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#1736")
    void shouldPreserveSnapshotFailureWhenWorkerShutdownIsInterrupted() throws Exception {
        insertDocuments("lifecycle", "items", new Document("_id", 1));
        insertDocuments("lifecycle", "other", new Document("_id", 1));
        final var otherReadStarted = new CountDownLatch(1);
        final var releaseRead = new CountDownLatch(1);
        final var snapshotThread = new AtomicReference<Thread>();
        final var interruptedAtFailure = new CompletableFuture<Boolean>();
        final var shutdownInterrupted = new CompletableFuture<Boolean>();
        final var failed = new CompletableFuture<Throwable>();
        final var snapshotLogger = (Logger) LoggerFactory.getLogger(MongoDbSnapshotChangeEventSource.class);
        final var observer = new LogInterceptor(MongoDbSnapshotChangeEventSource.class) {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().startsWith("Beginning snapshot at")) {
                    snapshotThread.set(Thread.currentThread());
                }
                else if ("Snapshot failed".equals(event.getFormattedMessage()) && Thread.currentThread() == snapshotThread.get()) {
                    interruptedAtFailure.complete(Thread.currentThread().isInterrupted());
                }
            }
        };
        try (var tracker = new ConnectionResourceTracker()) {
            tracker.commandListener = new CommandListener() {
                @Override
                public void commandStarted(CommandStartedEvent event) {
                    if (!"find".equals(event.getCommandName()) || !"lifecycle".equals(event.getDatabaseName())) {
                        return;
                    }
                    final var collection = event.getCommand().getString("find").getValue();
                    try {
                        if ("items".equals(collection)) {
                            // The invalid query must fail while another snapshot worker is still active.
                            assertThat(otherReadStarted.await(10, TimeUnit.SECONDS)).isTrue();
                        }
                        else if ("other".equals(collection)) {
                            otherReadStarted.countDown();
                            assertThat(releaseRead.await(30, TimeUnit.SECONDS)).isTrue();
                        }
                    }
                    catch (InterruptedException e) {
                        // shutdownNow() interrupts the remaining worker after the first query fails.
                        // Interrupt the coordinator before this worker can terminate, keeping it in the cleanup path.
                        snapshotThread.get().interrupt();
                        shutdownInterrupted.complete(true);
                        Thread.currentThread().interrupt();
                    }
                }
            };
            final var config = configuration(tracker).edit()
                    .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(MongoDbConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                    .with(MongoDbConnectorConfig.SNAPSHOT_FILTER_QUERY_BY_COLLECTION, "lifecycle.items")
                    .with("snapshot.collection.filter.overrides.lifecycle.items", "{\"$reviewFailure\": 1}")
                    .with(CommonConnectorConfig.MAX_RETRIES_ON_ERROR, 0)
                    .with(CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_MS, 30_000)
                    .build();
            try {
                start(MongoDbConnector.class, config, (success, message, error) -> failed.complete(error));
                assertThat(shutdownInterrupted.get(30, TimeUnit.SECONDS)).isTrue();
                assertThat(failed.get(30, TimeUnit.SECONDS)).hasRootCauseInstanceOf(MongoQueryException.class)
                        .rootCause().hasMessageContaining("$reviewFailure");
                assertThat(interruptedAtFailure.get(10, TimeUnit.SECONDS)).isTrue();
            }
            finally {
                releaseRead.countDown();
                stopConnector();
            }
            tracker.assertReleased();
        }
        finally {
            snapshotLogger.detachAppender(observer);
            observer.stop();
        }
    }

    @Test
    void shouldReleaseResourcesWhenCoordinatorStopIsInterrupted() throws Exception {
        insertDocuments("lifecycle", "items", new Document("_id", 1));
        final var reading = new CountDownLatch(1);
        final var releaseRead = new CountDownLatch(1);
        try (var tracker = new ConnectionResourceTracker()) {
            tracker.commandListener = blockSnapshotRead(reading, releaseRead, false);
            final var config = configuration(tracker).edit()
                    .with(MongoDbConnectorConfig.TASK_ID, 0)
                    .with(CommonConnectorConfig.CONNECTOR_CLASS, MongoDbConnector.class)
                    .with(CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_MS, 30_000)
                    .build();
            final var offsets = new KafkaMemoryOffsetProvider().create(config);
            final var task = new MongoDbConnectorTask();
            final var stopped = new CompletableFuture<Throwable>();
            final var stopper = new Thread(() -> {
                try {
                    task.stop();
                    stopped.complete(null);
                }
                catch (Throwable e) {
                    stopped.complete(e);
                }
            });
            offsets.configure(config);
            offsets.start();
            try {
                task.initialize(new KafkaConnectSourceTaskContextAdapter(config.asMap(), offsets.createReader("lifecycle")).getDelegate());
                task.start(config.asMap());
                assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();
                stopper.start();
                Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> stopper.getState() == Thread.State.TIMED_WAITING);
                stopper.interrupt();
                assertThat(stopped.get(10, TimeUnit.SECONDS)).isInstanceOf(ConnectException.class)
                        .hasMessage("Interrupted while stopping coordinator, failing the task");
                releaseRead.countDown();
                // BaseSourceTask skips doStop() on this path. Resources must still be released.
                Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(tracker::assertReleased);
            }
            finally {
                releaseRead.countDown();
                stopper.join(10_000);
                try {
                    task.stop();
                }
                finally {
                    offsets.stop();
                }
            }
        }
    }

    private CommandListener blockSnapshotRead(CountDownLatch reading, CountDownLatch releaseRead, boolean incrementalWorker) {
        return new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if ((!incrementalWorker || Thread.currentThread().getName().contains("-incremental-snapshot-"))
                        && event.getCommandName().equals("find") && event.getDatabaseName().equals("lifecycle")
                        && event.getCommand().getString("find").getValue().equals("items")) {
                    reading.countDown();
                    try {
                        releaseRead.await(30, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };
    }

    @Test
    void shouldRetainDifferentSourceAndSinkTopologyRequirements() {
        try (var standalone = MongoDbContainer.node().name("lifecycle-standalone").build();
                var tracker = new ConnectionResourceTracker()) {
            standalone.start();
            final var config = configuration(tracker).edit()
                    .with(MongoDbConnectorConfig.CONNECTION_STRING, "mongodb://" + standalone.getClientAddress())
                    .build();
            final var source = new MongoDbConnector().validate(config.asMap());
            assertThat(source.configValues()).anySatisfy(value -> assertThat(value.errorMessages())
                    .anySatisfy(message -> assertThat(message).contains("standalone server is not supported")));
            final var sink = new MongoDbSinkConnector().validate(config.asMap());
            assertThat(sink.configValues()).allSatisfy(value -> assertThat(value.errorMessages()).isEmpty());
            tracker.assertClientsCreated();
            tracker.assertReleased();
        }
    }
}
