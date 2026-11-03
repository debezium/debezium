/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.awaitility.Awaitility;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoChangeStreamCursor;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbEventMetadataProvider;
import io.debezium.connector.mongodb.MongoDbOffsetContext;
import io.debezium.connector.mongodb.MongoDbPartition;
import io.debezium.connector.mongodb.SourceInfo;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.document.DocumentReader;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.actions.snapshotting.CloseIncrementalSnapshotWindow;
import io.debezium.pipeline.signal.channels.SignalChannelReader;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.DatabaseSchema;

/**
 * Tests for dbz#2908.
 * <p>
 * The MongoDB connector emits its own {@code snapshot-window-open} / {@code snapshot-window-close} signals into the signalling
 * collection. The change event that carries such a signal is dispatched by {@code EventDispatcher#dispatchDataChangeEvent()},
 * which queues the signal on the {@link SourceSignalChannel} and then asks the {@link SignalProcessor} to run it on the
 * streaming thread via {@code processSourceSignal(partition)}.
 * <p>
 * The {@link SignalProcessor#process()} cycle must therefore not read the source signal channel, and it must not hold the
 * processor permit while it reads the external channels. Otherwise the window close runs on the signal processor thread,
 * concurrently with the streaming thread, and the shared {@link SourceInfo} of the partition is overwritten while the buffered
 * snapshot rows are emitted. That is what produced the reported
 * {@code DataException: Invalid value: null used for required field: "collection"}.
 * <p>
 * A window close that the streaming thread cannot execute, because the processor permit is held elsewhere, must not be
 * lost either. The {@link SignalProcessor} holds it, together with the partition of the change event that produced it, and
 * the streaming thread executes it at the next point where it processes source signals. Since no further signalling event
 * follows a window close, that point is the next heartbeat dispatch, which the streaming loop performs for every change
 * event.
 */
public class MongoDbIncrementalSnapshotSignalRaceTest {

    private static final String SNAPSHOT_COLLECTION = "inventory.products";

    /**
     * The {@code snapshot-window-close} signal must be executed by the streaming thread, never by the signal processor
     * thread. On {@code main} the signal processor thread wins the race and executes it.
     */
    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldExecuteSnapshotWindowCloseSignalOnTheStreamingThread() throws Exception {
        final MongoDbConnectorConfig connectorConfig = connectorConfig();

        final CountDownLatch kafkaReadStarted = new CountDownLatch(1);
        final CountDownLatch windowClosed = new CountDownLatch(1);
        final AtomicReference<String> executingThread = new AtomicReference<>();

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        // Mimics KafkaSignalChannel.read() blocking for signal.kafka.poll.timeout.ms.
        final BlockingSignalChannel kafkaChannel = new BlockingSignalChannel("kafka", kafkaReadStarted);

        final MongoDbPartition partition = new MongoDbPartition("mongo");
        final MongoDbOffsetContext offsetContext = MongoDbOffsetContext.empty(connectorConfig);

        final SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor = signalProcessor(connectorConfig, sourceChannel, kafkaChannel, partition,
                offsetContext);
        final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = dispatcher(connectorConfig, signalProcessor, windowClosed, executingThread);
        signalProcessor.registerSignalAction(CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(dispatcher));

        try {
            signalProcessor.start();

            assertThat(kafkaReadStarted.await(20, TimeUnit.SECONDS))
                    .as("the signal processor thread must be blocked reading the Kafka signal channel")
                    .isTrue();

            // This is what EventDispatcher.dispatchDataChangeEvent() does when the signalling collection
            // delivers the window close: queue the signal on the source channel, then let the streaming
            // thread process it.
            assertThat(sourceChannel.process(signalEvent("chunk-1-close", CloseIncrementalSnapshotWindow.NAME)))
                    .as("the window close signal must be accepted by the source signal channel")
                    .isTrue();

            // The streaming thread asks for the signal to be processed while the Kafka read is still in progress.
            signalProcessor.processSourceSignal(partition);

            // The Kafka read returns and the signal processor thread starts its next cycle.
            kafkaChannel.release.countDown();

            assertThat(windowClosed.await(30, TimeUnit.SECONDS))
                    .as("the snapshot-window-close signal must be processed")
                    .isTrue();

            assertThat(executingThread.get())
                    .as("the snapshot-window-close signal must be executed by the streaming thread")
                    .isEqualTo(Thread.currentThread().getName());
        }
        finally {
            kafkaChannel.release.countDown();
            signalProcessor.stop();
        }
    }

    /**
     * The source info of a snapshot READ record must carry the collection that was snapshotted. The document-less change
     * stream event and the emission of the buffered READ record are executed by the same thread, so the document-less event
     * cannot overwrite the source info in between.
     */
    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldKeepTheSnapshotCollectionInTheSourceInfoOfSnapshotRecords() throws Exception {
        final MongoDbConnectorConfig connectorConfig = connectorConfig();
        final MongoDbOffsetContext offsetContext = MongoDbOffsetContext.empty(connectorConfig);
        final CollectionId collectionId = CollectionId.parse(SNAPSHOT_COLLECTION);
        final MongoDbPartition partition = new MongoDbPartition("mongo");

        final CountDownLatch windowClosed = new CountDownLatch(1);
        final CountDownLatch noEventDuringEmission = new CountDownLatch(1);
        final AtomicBoolean emissionInProgress = new AtomicBoolean();
        final AtomicReference<Struct> emittedSource = new AtomicReference<>();
        final AtomicReference<String> executingThread = new AtomicReference<>();

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final CountDownLatch kafkaReadStarted = new CountDownLatch(1);
        final BlockingSignalChannel kafkaChannel = new BlockingSignalChannel("kafka", kafkaReadStarted);

        final SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor = signalProcessor(connectorConfig, sourceChannel, kafkaChannel, partition,
                offsetContext);
        final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = dispatcher(connectorConfig, signalProcessor, windowClosed, executingThread);

        // MongoDbIncrementalSnapshotChangeEventSource.sendEvent() sets the collection on the shared source info.
        final IncrementalSnapshotChangeEventSource<MongoDbPartition, CollectionId> incrementalSnapshotSource = dispatcher.getIncrementalSnapshotChangeEventSource();
        doAnswer(invocation -> {
            offsetContext.readEvent(collectionId, Instant.now());

            // MongoDbSnapshotRecordEmitter.emitReadRecord() reads the source info of the offset context when the record
            // is emitted. A document-less change stream event that arrives in between would clear the collection.
            emissionInProgress.set(true);
            noEventDuringEmission.await(1, TimeUnit.SECONDS);
            emittedSource.set(offsetContext.getSourceInfo());
            executingThread.set(Thread.currentThread().getName());
            windowClosed.countDown();
            return null;
        }).when(incrementalSnapshotSource).closeWindow(any(), any(), any());

        signalProcessor.registerSignalAction(CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(dispatcher));

        try {
            signalProcessor.start();

            assertThat(kafkaReadStarted.await(20, TimeUnit.SECONDS))
                    .as("the signal processor thread must be blocked reading the Kafka signal channel")
                    .isTrue();

            assertThat(sourceChannel.process(signalEvent("chunk-1-close", CloseIncrementalSnapshotWindow.NAME)))
                    .as("the window close signal must be accepted by the source signal channel")
                    .isTrue();

            // The streaming thread applies the document-less change stream event and then processes the signal.
            offsetContext.sourceInfo().noEvent(changeStreamCursorWithoutDocument());
            signalProcessor.processSourceSignal(partition);

            if (windowClosed.getCount() > 0) {
                // The signal processor thread held the permit, so the streaming thread could not process the signal.
                // The Kafka read returns and the signal processor thread starts its next cycle.
                kafkaChannel.release.countDown();

                // The streaming thread keeps applying the document-less change stream events while it streams, so the
                // window close that the signal processor thread performs races with them.
                Awaitility.await()
                        .atMost(30, TimeUnit.SECONDS)
                        .until(() -> {
                            offsetContext.sourceInfo().noEvent(changeStreamCursorWithoutDocument());
                            if (emissionInProgress.get()) {
                                noEventDuringEmission.countDown();
                            }
                            return windowClosed.getCount() == 0;
                        });
            }

            assertThat(windowClosed.await(30, TimeUnit.SECONDS))
                    .as("the snapshot-window-close signal must be processed")
                    .isTrue();

            assertThat(emittedSource.get().getString(SourceInfo.COLLECTION))
                    .as("the source info of a snapshot READ record must carry the snapshotted collection")
                    .isEqualTo(collectionId.name());

            assertThat(executingThread.get())
                    .as("the snapshot READ record must be emitted by the streaming thread")
                    .isEqualTo(Thread.currentThread().getName());
        }
        finally {
            kafkaChannel.release.countDown();
            signalProcessor.stop();
        }
    }

    /**
     * The window close signal must not be lost when the signal processor thread holds the processor permit. It is delayed
     * until the streaming thread can process it.
     */
    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldNotLoseTheSnapshotWindowCloseSignalWhenTheSemaphoreIsHeld() throws Exception {
        final MongoDbConnectorConfig connectorConfig = connectorConfig();

        final CountDownLatch windowClosed = new CountDownLatch(1);
        final AtomicReference<String> executingThread = new AtomicReference<>();
        final BlockingAction blockingAction = new BlockingAction();
        final AtomicInteger markerInvocations = new AtomicInteger();

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();

        final SignalChannelReader kafkaChannel = mock(SignalChannelReader.class);
        when(kafkaChannel.name()).thenReturn("kafka");
        when(kafkaChannel.read()).thenReturn(
                List.of(new SignalRecord("blocking", "blocking", "{}", Map.of())),
                List.of(new SignalRecord("marker", "marker", "{}", Map.of())),
                List.of());

        final MongoDbPartition partition = new MongoDbPartition("mongo");
        final MongoDbOffsetContext offsetContext = MongoDbOffsetContext.empty(connectorConfig);

        final SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor = signalProcessor(connectorConfig, sourceChannel, kafkaChannel, partition,
                offsetContext);
        signalProcessor.registerSignalAction("blocking", blockingAction);
        signalProcessor.registerSignalAction("marker", signalPayload -> {
            markerInvocations.incrementAndGet();
            return true;
        });

        final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = dispatcher(connectorConfig, signalProcessor, windowClosed, executingThread);
        signalProcessor.registerSignalAction(CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(dispatcher));

        try {
            signalProcessor.start();

            // The signal processor thread holds the permit while it executes the blocking action.
            assertThat(blockingAction.started.await(20, TimeUnit.SECONDS))
                    .as("the signal processor thread must be executing the blocking action")
                    .isTrue();

            assertThat(sourceChannel.process(signalEvent("chunk-1-close", CloseIncrementalSnapshotWindow.NAME)))
                    .as("the window close signal must be accepted by the source signal channel")
                    .isTrue();

            blockingAction.release.countDown();

            // The signal processor thread starts its next cycle, which reads the external channel again.
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(markerInvocations.get()).isEqualTo(1));

            assertThat(windowClosed.getCount())
                    .as("the signal processor thread must not execute the window close")
                    .isPositive();

            // The signal was delayed, not lost: the streaming thread closes the window.
            signalProcessor.processSourceSignal(partition);

            assertThat(windowClosed.await(30, TimeUnit.SECONDS))
                    .as("the snapshot-window-close signal must be processed")
                    .isTrue();
            assertThat(executingThread.get())
                    .as("the snapshot-window-close signal must be executed by the streaming thread")
                    .isEqualTo(Thread.currentThread().getName());
        }
        finally {
            blockingAction.release.countDown();
            signalProcessor.stop();
        }
    }

    /**
     * A window close that the streaming thread cannot execute, because the processor permit is held elsewhere, must not be
     * executed by the signal processor thread either. The {@link SignalProcessor} holds it, together with the partition of
     * the change event that produced it, until the streaming thread can execute it.
     */
    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldNotCloseTheSnapshotWindowWhileTheSemaphoreIsHeld() throws Exception {
        final MongoDbConnectorConfig connectorConfig = connectorConfig();

        final CountDownLatch windowClosed = new CountDownLatch(1);
        final AtomicReference<String> executingThread = new AtomicReference<>();
        final AtomicInteger markerInvocations = new AtomicInteger();

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final MongoDbPartition partition = new MongoDbPartition("mongo");
        final MongoDbOffsetContext offsetContext = MongoDbOffsetContext.empty(connectorConfig);

        final SignalChannelReader externalChannel = mock(SignalChannelReader.class);
        when(externalChannel.name()).thenReturn("kafka");
        when(externalChannel.read()).thenReturn(List.of(new SignalRecord("marker", "marker", "{}", Map.of())));

        final SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor = signalProcessor(connectorConfig, sourceChannel, externalChannel,
                partition, offsetContext);
        final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = dispatcher(connectorConfig, signalProcessor, windowClosed, executingThread);
        signalProcessor.registerSignalAction(CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(dispatcher));
        signalProcessor.registerSignalAction("marker", signalPayload -> {
            markerInvocations.incrementAndGet();
            return true;
        });

        try {
            deliverWindowCloseWhileTheSemaphoreIsHeld(signalProcessor, sourceChannel, partition);

            assertThat(windowClosed.getCount())
                    .as("the window must not be closed while the processor permit is held")
                    .isPositive();
            assertThat(executingThread.get())
                    .as("no signal action may have run while the processor permit was held")
                    .isNull();
            assertThat(sourceChannel.signals)
                    .as("the window close signal must have left the source channel and be held by the signal processor")
                    .isEmpty();

            // A processing cycle must not execute the source channel signal either.
            signalProcessor.process();

            assertThat(markerInvocations.get())
                    .as("the processing cycle must have run")
                    .isEqualTo(1);
            assertThat(windowClosed.getCount())
                    .as("the window must not be closed by the signal processor thread")
                    .isPositive();
        }
        finally {
            signalProcessor.stop();
        }
    }

    /**
     * A window close that the streaming thread could not execute must run at the next point where the streaming thread
     * processes source signals. No further signalling event follows a window close, so that point is the next heartbeat
     * dispatch, which the streaming loop performs for every change event.
     */
    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldCloseTheSnapshotWindowAfterTheSemaphoreWasHeld() throws Exception {
        final MongoDbConnectorConfig connectorConfig = connectorConfig();

        final CountDownLatch windowClosed = new CountDownLatch(1);
        final AtomicReference<String> executingThread = new AtomicReference<>();
        final AtomicReference<MongoDbPartition> closedPartition = new AtomicReference<>();

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final MongoDbPartition partition = new MongoDbPartition("mongo");
        final MongoDbOffsetContext offsetContext = MongoDbOffsetContext.empty(connectorConfig);

        final SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor = signalProcessor(connectorConfig, sourceChannel, mock(SignalChannelReader.class),
                partition, offsetContext);
        final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = dispatcher(connectorConfig, signalProcessor, windowClosed, executingThread);
        final IncrementalSnapshotChangeEventSource<MongoDbPartition, CollectionId> incrementalSnapshotSource = dispatcher.getIncrementalSnapshotChangeEventSource();
        doAnswer(invocation -> {
            closedPartition.set(invocation.getArgument(0));
            executingThread.set(Thread.currentThread().getName());
            windowClosed.countDown();
            return null;
        }).when(incrementalSnapshotSource).closeWindow(any(), any(), any());

        signalProcessor.registerSignalAction(CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(dispatcher));

        try {
            deliverWindowCloseWhileTheSemaphoreIsHeld(signalProcessor, sourceChannel, partition);

            assertThat(windowClosed.getCount())
                    .as("the window must not be closed while the processor permit is held")
                    .isPositive();

            // No further signalling event arrives: the streaming thread reaches its next tick, the heartbeat dispatch.
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);

            assertThat(windowClosed.await(30, TimeUnit.SECONDS))
                    .as("the snapshot-window-close signal must be executed at the next streaming thread tick")
                    .isTrue();
            assertThat(executingThread.get())
                    .as("the snapshot-window-close signal must be executed by the streaming thread")
                    .isEqualTo(Thread.currentThread().getName());
            assertThat(closedPartition.get())
                    .as("the window must be closed with the partition of the change event that produced the signal")
                    .isSameAs(partition);
        }
        finally {
            signalProcessor.stop();
        }
    }

    /**
     * A window close that the streaming thread could not execute must also run at the idle tick of a streaming source
     * that only performs the always heartbeat dispatch. This is the tick of the Oracle OpenLogReplicator source, whose
     * commit and checkpoint events are the only points where it reaches the dispatcher.
     */
    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldCloseTheSnapshotWindowOnTheAlwaysDispatchHeartbeatEvent() throws Exception {
        final MongoDbConnectorConfig connectorConfig = connectorConfig();

        final CountDownLatch windowClosed = new CountDownLatch(1);
        final AtomicReference<String> executingThread = new AtomicReference<>();
        final AtomicReference<MongoDbPartition> closedPartition = new AtomicReference<>();

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final MongoDbPartition partition = new MongoDbPartition("mongo");
        final MongoDbOffsetContext offsetContext = MongoDbOffsetContext.empty(connectorConfig);

        final SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor = signalProcessor(connectorConfig, sourceChannel, mock(SignalChannelReader.class),
                partition, offsetContext);
        final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = dispatcher(connectorConfig, signalProcessor, windowClosed, executingThread);
        final IncrementalSnapshotChangeEventSource<MongoDbPartition, CollectionId> incrementalSnapshotSource = dispatcher.getIncrementalSnapshotChangeEventSource();
        doAnswer(invocation -> {
            closedPartition.set(invocation.getArgument(0));
            executingThread.set(Thread.currentThread().getName());
            windowClosed.countDown();
            return null;
        }).when(incrementalSnapshotSource).closeWindow(any(), any(), any());

        signalProcessor.registerSignalAction(CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(dispatcher));

        try {
            deliverWindowCloseWhileTheSemaphoreIsHeld(signalProcessor, sourceChannel, partition);

            assertThat(windowClosed.getCount())
                    .as("the window must not be closed while the processor permit is held")
                    .isPositive();

            // No further signalling event arrives: the streaming thread reaches its idle tick.
            dispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);

            assertThat(windowClosed.await(30, TimeUnit.SECONDS))
                    .as("the snapshot-window-close signal must be executed at the next streaming thread tick")
                    .isTrue();
            assertThat(executingThread.get())
                    .as("the snapshot-window-close signal must be executed by the streaming thread")
                    .isEqualTo(Thread.currentThread().getName());
            assertThat(closedPartition.get())
                    .as("the window must be closed with the partition of the change event that produced the signal")
                    .isSameAs(partition);
        }
        finally {
            signalProcessor.stop();
        }
    }

    /**
     * A pending window close must only be executed by the thread that received it. The snapshot thread and a signal
     * action reach the always heartbeat dispatch as well, and neither of them may execute the window close.
     */
    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldNotCloseTheSnapshotWindowOnTheHeartbeatDispatchOfAnotherThread() throws Exception {
        final MongoDbConnectorConfig connectorConfig = connectorConfig();

        final CountDownLatch windowClosed = new CountDownLatch(1);
        final AtomicReference<String> executingThread = new AtomicReference<>();

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final MongoDbPartition partition = new MongoDbPartition("mongo");
        final MongoDbOffsetContext offsetContext = MongoDbOffsetContext.empty(connectorConfig);

        final SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor = signalProcessor(connectorConfig, sourceChannel, mock(SignalChannelReader.class),
                partition, offsetContext);
        final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = dispatcher(connectorConfig, signalProcessor, windowClosed, executingThread);

        signalProcessor.registerSignalAction(CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(dispatcher));

        try {
            deliverWindowCloseWhileTheSemaphoreIsHeld(signalProcessor, sourceChannel, partition);

            assertThat(windowClosed.getCount())
                    .as("the window must not be closed while the processor permit is held")
                    .isPositive();

            // Another thread reaches the heartbeat dispatches; it must not execute the pending window close.
            runOnAnotherThread(() -> {
                dispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                return null;
            });

            assertThat(windowClosed.getCount())
                    .as("the window must not be closed by a thread that did not receive the signal")
                    .isPositive();

            // The streaming thread closes the window at its next tick.
            dispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);

            assertThat(windowClosed.await(30, TimeUnit.SECONDS))
                    .as("the snapshot-window-close signal must be executed at the next streaming thread tick")
                    .isTrue();
            assertThat(executingThread.get())
                    .as("the snapshot-window-close signal must be executed by the streaming thread")
                    .isEqualTo(Thread.currentThread().getName());
        }
        finally {
            signalProcessor.stop();
        }
    }

    private static MongoDbConnectorConfig connectorConfig() {
        return new MongoDbConnectorConfig(TestHelper.getConfiguration().edit()
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "kafka,source")
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 100)
                .with(CommonConnectorConfig.SIGNAL_PROCESSOR_SEMAPHORE_WAIT_MS, 500)
                .build());
    }

    private static SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor(MongoDbConnectorConfig connectorConfig,
                                                                                           SourceSignalChannel sourceChannel,
                                                                                           SignalChannelReader externalChannel,
                                                                                           MongoDbPartition partition,
                                                                                           MongoDbOffsetContext offsetContext) {
        return new SignalProcessor<>(
                MongoDbConnector.class,
                connectorConfig,
                Map.of(),
                List.of(sourceChannel, externalChannel),
                DocumentReader.defaultReader(),
                Offsets.of(partition, offsetContext));
    }

    /**
     * Builds the real {@link EventDispatcher} the MongoDB connector uses, with the real {@link SignalProcessor} and the
     * real {@link SourceSignalChannel}. The database schema and the incremental snapshot source need a live MongoDB, so
     * they are mocks.
     */
    private static EventDispatcher<MongoDbPartition, CollectionId> dispatcher(MongoDbConnectorConfig connectorConfig,
                                                                              SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor,
                                                                              CountDownLatch windowClosed,
                                                                              AtomicReference<String> executingThread)
            throws InterruptedException {
        @SuppressWarnings("unchecked")
        final DatabaseSchema<CollectionId> databaseSchema = mock(DatabaseSchema.class);
        @SuppressWarnings("unchecked")
        final ChangeEventQueue<DataChangeEvent> changeEventQueue = mock(ChangeEventQueue.class);

        @SuppressWarnings("unchecked")
        final IncrementalSnapshotChangeEventSource<MongoDbPartition, CollectionId> incrementalSnapshotSource = mock(IncrementalSnapshotChangeEventSource.class);
        doAnswer(invocation -> {
            executingThread.set(Thread.currentThread().getName());
            windowClosed.countDown();
            return null;
        }).when(incrementalSnapshotSource).closeWindow(any(), any(), any());

        final CdcSourceTaskContext<CommonConnectorConfig> cdcSourceTaskContext = mock(CdcSourceTaskContext.class);
        when(cdcSourceTaskContext.getTaskId()).thenReturn("0");
        when(cdcSourceTaskContext.getConnectorLogicalName()).thenReturn("test");
        when(cdcSourceTaskContext.getConnectorPluginName()).thenReturn("mongodb");
        when(cdcSourceTaskContext.getRunId()).thenReturn(UUID.fromString("671711f6-5517-4c69-806b-87c1b034ee7b"));

        final EventDispatcher<MongoDbPartition, CollectionId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                connectorConfig.getTopicNamingStrategy(MongoDbConnectorConfig.TOPIC_NAMING_STRATEGY),
                databaseSchema,
                changeEventQueue,
                collectionId -> true,
                DataChangeEvent::new,
                new MongoDbEventMetadataProvider(),
                Heartbeat.ScheduledHeartbeat.NOOP_HEARTBEAT,
                connectorConfig.schemaNameAdjuster(),
                signalProcessor,
                new DebeziumHeaderProducer(cdcSourceTaskContext));

        dispatcher.setIncrementalSnapshotChangeEventSource(Optional.of(incrementalSnapshotSource));

        return dispatcher;
    }

    /**
     * Returns the semaphore of the signal processor, so that the test can hold its permit while a signal is delivered.
     */
    private static Semaphore signalProcessorSemaphore(SignalProcessor<?, ?> processor) throws Exception {
        final Field field = SignalProcessor.class.getDeclaredField("semaphore");
        field.setAccessible(true);
        return (Semaphore) field.get(processor);
    }

    /**
     * Delivers a window close signal to the signal processor while the processor permit is held, so that the
     * streaming thread cannot execute it and it stays pending.
     */
    private static void deliverWindowCloseWhileTheSemaphoreIsHeld(SignalProcessor<MongoDbPartition, MongoDbOffsetContext> signalProcessor,
                                                                  SourceSignalChannel sourceChannel,
                                                                  MongoDbPartition partition)
            throws Exception {
        final Semaphore semaphore = signalProcessorSemaphore(signalProcessor);
        semaphore.acquire();
        try {
            assertThat(sourceChannel.process(signalEvent("chunk-1-close", CloseIncrementalSnapshotWindow.NAME)))
                    .as("the window close signal must be accepted by the source signal channel")
                    .isTrue();

            // The streaming thread delivers the signal, but it cannot take the processor permit.
            signalProcessor.processSourceSignal(partition);
        }
        finally {
            semaphore.release();
        }
    }

    /**
     * Runs the given action on a thread other than the current one and waits for it to finish.
     */
    private static void runOnAnotherThread(Callable<?> action) throws Exception {
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                action.call();
            }
            catch (Exception e) {
                failure.set(e);
            }
        });
        thread.start();
        thread.join();
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    /**
     * Builds the change event the MongoDB connector delivers for a document of the signalling collection. The MongoDB
     * connector parses the {@code after} field as a JSON document with the three fields {@code _id}, {@code type} and
     * {@code data}, in that order.
     */
    private static Struct signalEvent(String id, String type) {
        final Schema envelopeSchema = SchemaBuilder.struct().name("io.debezium.connector.mongodb.signal.envelope")
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.AFTER, Schema.STRING_SCHEMA)
                .build();

        return new Struct(envelopeSchema)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code())
                .put(Envelope.FieldName.AFTER, "{\"_id\":\"" + id + "\",\"type\":\"" + type + "\",\"data\":\"\"}");
    }

    /**
     * A change stream cursor that carries a resume token but no document, which is what the MongoDB connector reports for
     * every empty poll of the change stream.
     */
    private static MongoChangeStreamCursor<?> changeStreamCursorWithoutDocument() {
        final var cursor = mock(MongoChangeStreamCursor.class);
        when(cursor.getResumeToken()).thenReturn(new BsonDocument("_data", new BsonString("8257E0A0F000000000000000")));
        return cursor;
    }

    /**
     * A signal channel whose read blocks, mimicking {@code KafkaSignalChannel.read()} waiting for the poll timeout.
     */
    private static class BlockingSignalChannel implements SignalChannelReader {

        private final String name;
        private final CountDownLatch readStarted;
        private final CountDownLatch release = new CountDownLatch(1);

        BlockingSignalChannel(String name, CountDownLatch readStarted) {
            this.name = name;
            this.readStarted = readStarted;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void init(CommonConnectorConfig connectorConfig) {

        }

        @Override
        public List<SignalRecord> read() {
            readStarted.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return List.of();
        }

        @Override
        public void close() {
            release.countDown();
        }
    }

    /**
     * A signal action that blocks the thread executing it, so that the processor permit is held for a while.
     */
    private static class BlockingAction implements SignalAction<MongoDbPartition> {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public boolean arrived(SignalPayload<MongoDbPartition> signalPayload) throws InterruptedException {
            started.countDown();
            release.await(30, TimeUnit.SECONDS);
            return true;
        }
    }
}
