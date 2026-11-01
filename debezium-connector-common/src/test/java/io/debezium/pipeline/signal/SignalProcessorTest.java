/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.document.DocumentReader;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.signal.actions.Log;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.channels.SignalChannelReader;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

import ch.qos.logback.classic.Level;

public class SignalProcessorTest {

    private SignalProcessor<TestPartition, OffsetContext> signalProcess;
    private final DocumentReader documentReader = DocumentReader.defaultReader();

    private Offsets<TestPartition, OffsetContext> initialOffset;

    @BeforeEach
    public void setUp() {
        TestOffset testOffset = new TestOffset(new BaseSourceInfo(baseConfig()) {
            @Override
            protected Instant timestamp() {
                return Instant.now();
            }

            @Override
            protected String database() {
                return "test_db";
            }
        });

        initialOffset = Offsets.of(new TestPartition(), testOffset);
    }

    @AfterEach
    public void tearDown() {
        // Restore the inherited level in case a test raised it to DEBUG
        new LogInterceptor(SignalProcessor.class).setLoggerLevel(SignalProcessor.class, null);
    }

    /**
     * Returns an interceptor for {@link SignalProcessor} with its logger raised to DEBUG, so tests can
     * observe the deferral of synchronous signals.
     */
    private static LogInterceptor signalProcessorDebugLog() {
        final LogInterceptor log = new LogInterceptor(SignalProcessor.class);
        log.setLoggerLevel(SignalProcessor.class, Level.DEBUG);
        return log;
    }

    @Test
    public void shouldExecuteLog() throws InterruptedException {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("log1", "log", "{\"message\": \"signallog {}\"}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = new LogInterceptor(Log.class);

        signalProcess = new SignalProcessor<>(SourceConnector.class,
                baseConfig(),
                Map.of(Log.NAME, new Log<>()),
                List.of(genericChannel), documentReader, initialOffset);

        signalProcess.start();

        Awaitility.await()
                .atMost(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> log.containsMessage("signallog {LSN=12345}"));

        signalProcess.stop();

        assertThat(log.containsMessage("signallog {LSN=12345}")).isTrue();
    }

    @Test
    public void onlyEnabledConnectorShouldExecute() throws InterruptedException {

        final SignalChannelReader genericChannel1 = mock(SignalChannelReader.class);

        when(genericChannel1.name()).thenReturn("generic1");
        when(genericChannel1.read()).thenReturn(
                List.of(new SignalRecord("log1", "log", "{\"message\": \"signallog {}\"}", Map.of("channelOffset", -1L))),
                List.of());

        final SignalChannelReader genericChannel2 = mock(SignalChannelReader.class);
        when(genericChannel2.name()).thenReturn("generic2");
        when(genericChannel2.read()).thenReturn(
                List.of(new SignalRecord("log1", "log", "{\"message\": \"signallog {}\"}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = new LogInterceptor(Log.class);

        signalProcess = new SignalProcessor<>(SourceConnector.class,
                baseConfig(Map.of(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS.name(), "generic1")),
                Map.of(Log.NAME, new Log<>()),
                List.of(genericChannel1, genericChannel2), documentReader, initialOffset);

        signalProcess.start();

        Awaitility.await()
                .atMost(300, TimeUnit.MILLISECONDS)
                .until(() -> log.containsMessage("signallog {LSN=12345}"));

        signalProcess.stop();

        assertThat(log.countOccurrences("signallog {}")).isEqualTo(1);
    }

    @Test
    public void shouldIgnoreInvalidSignalType() throws InterruptedException {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("log1", "invalidType", "{\"message\": \"signallog {}\"}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = new LogInterceptor(SignalProcessor.class);

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(), Map.of(), List.of(genericChannel), documentReader, initialOffset);

        signalProcess.start();

        Awaitility.await()
                .atMost(200, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(log.containsMessage("Signal 'log1' has been received but the type 'invalidType' is not recognized")).isTrue());

        signalProcess.stop();
    }

    @Test
    public void shouldIgnoreUnparseableData() throws InterruptedException {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("log1", "log", "{\"message: \"signallog\"}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = new LogInterceptor(SignalProcessor.class);

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(), Map.of(Log.NAME, new Log<>()), List.of(genericChannel), documentReader, initialOffset);

        signalProcess.start();

        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertThat(log.containsMessage("Signal 'log1' has been received but the data '{\"message: \"signallog\"}' cannot be parsed")).isTrue());

        signalProcess.stop();
    }

    @Test
    public void shouldRegisterAdditionalAction() throws InterruptedException {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("log1", "custom", "{\"v\": 5}", Map.of("channelOffset", -1L))),
                List.of());

        final AtomicInteger called = new AtomicInteger();
        final SignalAction<TestPartition> testAction = signalPayload -> {
            called.set(signalPayload.data.getInteger("v"));
            return true;
        };

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(), Map.of(), List.of(genericChannel), documentReader, initialOffset);

        signalProcess.registerSignalAction("custom", testAction);

        signalProcess.start();

        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(called.intValue()).isEqualTo(5));

        signalProcess.stop();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldDeferSynchronousActionUntilStreamingSourceProcessesSignals() throws InterruptedException {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("sync1", "custom", "{\"v\": 7}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = signalProcessorDebugLog();
        final AtomicInteger called = new AtomicInteger();
        final AtomicReference<Thread> executingThread = new AtomicReference<>();
        final AtomicReference<SignalPayload<TestPartition>> receivedPayload = new AtomicReference<>();
        final SignalAction<TestPartition> testAction = new SynchronousAction(signalPayload -> {
            called.set(signalPayload.data.getInteger("v"));
            executingThread.set(Thread.currentThread());
            receivedPayload.set(signalPayload);
            return true;
        });

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(), Map.of("custom", testAction), List.of(genericChannel), documentReader,
                initialOffset);

        signalProcess.start();

        // The processor reads the signal on its executor thread, but must not execute it there
        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(log.containsMessage("Signal 'sync1' of type 'custom' deferred until the streaming source processes synchronous signals"))
                        .isTrue());
        assertThat(called.intValue()).isZero();

        // The streaming source drains the queue on its own thread
        signalProcess.processSynchronousSignals();

        assertThat(called.intValue()).isEqualTo(7);
        assertThat(executingThread.get()).isSameAs(Thread.currentThread());
        assertThat(receivedPayload.get().id).isEqualTo("sync1");
        assertThat(receivedPayload.get().partition).isSameAs(initialOffset.getTheOnlyPartition());
        assertThat(receivedPayload.get().offsetContext).isSameAs(initialOffset.getTheOnlyOffset());

        // Draining again is a no-op
        signalProcess.processSynchronousSignals();
        assertThat(called.intValue()).isEqualTo(7);

        signalProcess.stop();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldExecuteSynchronousSignalsInArrivalOrder() throws InterruptedException {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("first", "custom", "{}", Map.of("channelOffset", -1L)),
                        new SignalRecord("second", "custom", "{}", Map.of("channelOffset", -1L))),
                List.of(new SignalRecord("third", "custom", "{}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = signalProcessorDebugLog();
        final List<String> executed = new CopyOnWriteArrayList<>();
        final SignalAction<TestPartition> testAction = new SynchronousAction(signalPayload -> executed.add(signalPayload.id));

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(), Map.of("custom", testAction), List.of(genericChannel), documentReader,
                initialOffset);

        signalProcess.start();

        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(log.containsMessage("Signal 'third' of type 'custom' deferred until the streaming source processes synchronous signals"))
                        .isTrue());
        assertThat(executed).isEmpty();

        signalProcess.processSynchronousSignals();

        assertThat(executed).containsExactly("first", "second", "third");

        signalProcess.stop();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldLimitSynchronousSignalsExecutedPerCall() throws InterruptedException {

        final int max = 4;
        final int total = max + 3;
        final List<SignalRecord> burst = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            burst.add(new SignalRecord("burst-" + i, "custom", "{}", Map.of("channelOffset", -1L)));
        }

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(burst, List.of());

        final LogInterceptor log = signalProcessorDebugLog();
        final List<String> executed = new CopyOnWriteArrayList<>();
        final SignalAction<TestPartition> testAction = new SynchronousAction(signalPayload -> executed.add(signalPayload.id));

        final CommonConnectorConfig config = baseConfig(Map.of(CommonConnectorConfig.SIGNAL_SYNCHRONOUS_BATCH_SIZE.name(), max));
        signalProcess = new SignalProcessor<>(SourceConnector.class, config, Map.of("custom", testAction), List.of(genericChannel), documentReader,
                initialOffset);

        signalProcess.start();

        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(log.containsMessage(
                        "Signal 'burst-" + (total - 1) + "' of type 'custom' deferred until the streaming source processes synchronous signals")).isTrue());
        assertThat(executed).isEmpty();

        // The first call executes only up to the configured maximum, in arrival order, and leaves the remainder queued
        signalProcess.processSynchronousSignals();

        assertThat(executed).containsExactly("burst-0", "burst-1", "burst-2", "burst-3");
        assertThat(log.containsMessage("Reached the limit of " + max + " synchronous signals per call; 3 signal(s) deferred until the next call")).isTrue();

        // The next call drains the remainder
        signalProcess.processSynchronousSignals();

        assertThat(executed).containsExactly("burst-0", "burst-1", "burst-2", "burst-3", "burst-4", "burst-5", "burst-6");

        // Nothing is left, so a further call is a no-op
        signalProcess.processSynchronousSignals();
        assertThat(executed).hasSize(total);

        signalProcess.stop();
        assertThat(log.containsWarnMessage("SignalProcessor stopped with")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldDefaultSynchronousBatchSizeToTen() {
        assertThat(baseConfig().getSignalSynchronousBatchSize()).isEqualTo(10);
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldContinueWithNextSynchronousSignalWhenActionFails() throws InterruptedException {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("failing", "custom", "{}", Map.of("channelOffset", -1L)),
                        new SignalRecord("succeeding", "custom", "{}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = signalProcessorDebugLog();
        final List<String> executed = new CopyOnWriteArrayList<>();
        final SignalAction<TestPartition> testAction = new SynchronousAction(signalPayload -> {
            if ("failing".equals(signalPayload.id)) {
                throw new IllegalStateException("boom");
            }
            return executed.add(signalPayload.id);
        });

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(), Map.of("custom", testAction), List.of(genericChannel), documentReader,
                initialOffset);

        signalProcess.start();

        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(
                        log.containsMessage("Signal 'succeeding' of type 'custom' deferred until the streaming source processes synchronous signals")).isTrue());

        signalProcess.processSynchronousSignals();

        assertThat(executed).containsExactly("succeeding");
        assertThat(log.containsWarnMessage("Action custom failed.")).isTrue();

        signalProcess.stop();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldSkipSynchronousSignalWhenPartitionIsNoLongerManaged() throws InterruptedException {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("orphan", "custom", "{}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = signalProcessorDebugLog();
        final AtomicInteger called = new AtomicInteger();
        final SignalAction<TestPartition> testAction = new SynchronousAction(signalPayload -> {
            called.incrementAndGet();
            return true;
        });

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(), Map.of("custom", testAction), List.of(genericChannel), documentReader,
                initialOffset);

        signalProcess.start();

        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(log.containsMessage("Signal 'orphan' of type 'custom' deferred until the streaming source processes synchronous signals"))
                        .isTrue());

        // The partition is dropped before the streaming source gets to the signal
        signalProcess.setContext(null);
        signalProcess.processSynchronousSignals();

        assertThat(called.intValue()).isZero();
        assertThat(log.containsWarnMessage("Signal 'orphan' of type 'custom' references partition")).isTrue();

        signalProcess.stop();
    }

    @Test
    @FixFor("debezium/dbz#2577")
    public void shouldWarnAboutUnexecutedSynchronousSignalsOnStop() throws InterruptedException {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("pending", "custom", "{}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = signalProcessorDebugLog();
        final SignalAction<TestPartition> testAction = new SynchronousAction(signalPayload -> true);

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(), Map.of("custom", testAction), List.of(genericChannel), documentReader,
                initialOffset);

        signalProcess.start();

        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertThat(log.containsMessage("Signal 'pending' of type 'custom' deferred until the streaming source processes synchronous signals"))
                                .isTrue());

        signalProcess.stop();

        assertThat(log.containsWarnMessage("SignalProcessor stopped with 1 synchronous signal(s) that were never executed: [pending]")).isTrue();
    }

    @Test
    @FixFor({ "debezium/dbz#2458", "debezium/dbz#2908" })
    public void shouldSkipProcessingWhenSemaphoreCannotBeAcquired() throws Exception {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("log1", "log", "{\"message\": \"signallog {}\"}", Map.of("channelOffset", -1L))),
                List.of());

        final LogInterceptor log = new LogInterceptor(Log.class);

        signalProcess = new SignalProcessor<>(SourceConnector.class,
                baseConfig(Map.of(CommonConnectorConfig.SIGNAL_PROCESSOR_SEMAPHORE_WAIT_MS.name(), 1L)),
                Map.of(Log.NAME, new Log<>()),
                List.of(genericChannel), documentReader, initialOffset);

        // Hold the single permit so processing cannot acquire it within the configured wait and must skip
        // the cycle rather than run the signal action without holding the lock.
        final Semaphore semaphore = signalProcessorSemaphore(signalProcess);
        semaphore.acquire();
        try {
            signalProcess.process();

            // The channel is read outside the permit, so the record has already left the channel.
            verify(genericChannel).read();
            assertThat(log.containsMessage("signallog {LSN=12345}")).isFalse();
        }
        finally {
            semaphore.release();
        }

        // The record that could not be executed before is not lost, the next cycle executes it.
        signalProcess.process();
        assertThat(log.containsMessage("signallog {LSN=12345}")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldNotLoseAnExternalChannelSignalWhenTheSemaphoreCannotBeAcquired() throws Exception {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("log1", "custom", "{}", Map.of("channelOffset", -1L))),
                List.of());

        final RecordingAction action = new RecordingAction();

        signalProcess = new SignalProcessor<>(SourceConnector.class,
                baseConfig(Map.of(CommonConnectorConfig.SIGNAL_PROCESSOR_SEMAPHORE_WAIT_MS.name(), 1L)),
                Map.of("custom", action),
                List.of(genericChannel), documentReader, initialOffset);

        final Semaphore semaphore = signalProcessorSemaphore(signalProcess);
        semaphore.acquire();
        try {
            signalProcess.process();

            // The channel is read outside the permit, so the record has already left the channel.
            verify(genericChannel).read();
            assertThat(action.invocations.get())
                    .as("the signal action must not be executed without the permit")
                    .isZero();
        }
        finally {
            semaphore.release();
        }

        // The record that could not be executed before is not lost, the next cycle executes it.
        signalProcess.process();
        assertThat(action.invocations.get())
                .as("the record read by the skipped cycle must be executed by the next cycle")
                .isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldExecuteSourceChannelSignalOnTheStreamingThread() throws Exception {

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final BlockingSignalChannel kafkaChannel = new BlockingSignalChannel("kafka");
        final RecordingAction action = new RecordingAction();

        signalProcess = new SignalProcessor<>(SourceConnector.class,
                baseConfig(Map.of(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS.name(), "source,kafka",
                        CommonConnectorConfig.SIGNAL_PROCESSOR_SEMAPHORE_WAIT_MS.name(), 500L)),
                Map.of("custom", action),
                List.of(sourceChannel, kafkaChannel), documentReader, initialOffset);

        try {
            signalProcess.start();

            assertThat(kafkaChannel.readStarted.await(20, TimeUnit.SECONDS))
                    .as("the signal processor thread must be blocked reading the Kafka signal channel")
                    .isTrue();

            // This is what EventDispatcher#dispatchDataChangeEvent() does when the signalling collection delivers the signal.
            assertThat(sourceChannel.process(sourceSignalEvent("signal-1", "custom")))
                    .as("the signal must be accepted by the source signal channel")
                    .isTrue();

            // The streaming thread asks for the signal to be processed while the Kafka read is still in progress.
            signalProcess.processSourceSignal(initialOffset.getTheOnlyPartition());

            // The Kafka read returns and the signal processor thread starts its next cycle.
            kafkaChannel.release.countDown();

            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(action.invocations.get()).isEqualTo(1));

            assertThat(action.executingThread.get())
                    .as("the source channel signal must be executed by the streaming thread")
                    .isSameAs(Thread.currentThread());
        }
        finally {
            kafkaChannel.release.countDown();
            signalProcess.stop();
        }
    }

    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldNotDrainTheSourceChannelOnTheSignalProcessorThread() throws Exception {

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final RecordingAction action = new RecordingAction();

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(),
                Map.of("custom", action), List.of(sourceChannel), documentReader, initialOffset);

        assertThat(sourceChannel.process(sourceSignalEvent("signal-1", "custom")))
                .as("the signal must be accepted by the source signal channel")
                .isTrue();

        signalProcess.process();

        assertThat(action.invocations.get())
                .as("the periodic cycle must not execute a source channel signal")
                .isZero();
        assertThat(sourceChannel.signals)
                .as("the source channel signal must still be queued")
                .hasSize(1);
    }

    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldExecuteThePendingSourceChannelSignalOnTheNextStreamingThreadCall() throws Exception {

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final BlockingAction blockingAction = new BlockingAction();
        final RecordingAction markerAction = new RecordingAction();
        final RecordingAction sourceAction = new RecordingAction();

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("blocking", "blocking", "{}", Map.of("channelOffset", -1L))),
                List.of(new SignalRecord("marker", "marker", "{}", Map.of("channelOffset", -1L))),
                List.of());

        signalProcess = new SignalProcessor<>(SourceConnector.class,
                baseConfig(Map.of(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS.name(), "source,generic")),
                Map.of("blocking", blockingAction, "marker", markerAction, "custom", sourceAction),
                List.of(sourceChannel, genericChannel), documentReader, initialOffset);

        try {
            signalProcess.start();

            // The signal processor thread holds the permit while it executes the blocking action.
            assertThat(blockingAction.started.await(20, TimeUnit.SECONDS))
                    .as("the signal processor thread must be executing the blocking action")
                    .isTrue();

            // The source channel signal is enqueued while the permit is held.
            assertThat(sourceChannel.process(sourceSignalEvent("signal-1", "custom")))
                    .as("the signal must be accepted by the source signal channel")
                    .isTrue();

            blockingAction.release.countDown();

            // The signal processor thread starts its next cycle, which reads the external channel again.
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(markerAction.invocations.get()).isEqualTo(1));

            assertThat(sourceAction.invocations.get())
                    .as("the source channel signal must not be executed by the signal processor thread")
                    .isZero();

            // The signal was delayed, not lost: the streaming thread executes it.
            signalProcess.processSourceSignal(initialOffset.getTheOnlyPartition());

            assertThat(sourceAction.invocations.get()).isEqualTo(1);
            assertThat(sourceAction.executingThread.get())
                    .as("the source channel signal must be executed by the streaming thread")
                    .isSameAs(Thread.currentThread());
        }
        finally {
            blockingAction.release.countDown();
            signalProcess.stop();
        }
    }

    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldNotHoldTheSemaphoreWhileReadingAnExternalChannel() throws Exception {

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final BlockingSignalChannel kafkaChannel = new BlockingSignalChannel("kafka");
        final RecordingAction action = new RecordingAction();

        signalProcess = new SignalProcessor<>(SourceConnector.class,
                baseConfig(Map.of(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS.name(), "source,kafka",
                        CommonConnectorConfig.SIGNAL_PROCESSOR_SEMAPHORE_WAIT_MS.name(), 500L)),
                Map.of("custom", action),
                List.of(sourceChannel, kafkaChannel), documentReader, initialOffset);

        try {
            signalProcess.start();

            assertThat(kafkaChannel.readStarted.await(20, TimeUnit.SECONDS))
                    .as("the signal processor thread must be blocked reading the Kafka signal channel")
                    .isTrue();

            assertThat(sourceChannel.process(sourceSignalEvent("signal-1", "custom")))
                    .as("the signal must be accepted by the source signal channel")
                    .isTrue();

            // The streaming thread must be able to acquire the permit while the Kafka read is still blocked.
            signalProcess.processSourceSignal(initialOffset.getTheOnlyPartition());

            assertThat(action.invocations.get())
                    .as("the source channel signal must be executed while the external channel read is blocked")
                    .isEqualTo(1);
            assertThat(kafkaChannel.readCompleted.getCount())
                    .as("the external channel read must still be in progress")
                    .isPositive();
        }
        finally {
            kafkaChannel.release.countDown();
            signalProcess.stop();
        }
    }

    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldExecuteExternalChannelSignalOnTheSignalProcessorThread() throws Exception {

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenReturn(
                List.of(new SignalRecord("log1", "custom", "{}", Map.of("channelOffset", -1L))),
                List.of());

        final RecordingAction action = new RecordingAction();

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(),
                Map.of("custom", action), List.of(genericChannel), documentReader, initialOffset);

        try {
            signalProcess.start();

            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(action.invocations.get()).isEqualTo(1));

            assertThat(action.executingThread.get())
                    .as("the external channel signal must be executed by the signal processor thread")
                    .isNotSameAs(Thread.currentThread());
            assertThat(action.executingThread.get().getName())
                    .as("the external channel signal must be executed by the signal processor thread")
                    .contains("SignalProcessor");
        }
        finally {
            signalProcess.stop();
        }
    }

    @Test
    @FixFor("debezium/dbz#2908")
    public void shouldNotExecuteASourceChannelSignalWithoutTheStreamingThread() throws Exception {

        final SourceSignalChannel sourceChannel = new SourceSignalChannel();
        final RecordingAction action = new RecordingAction();
        final RecordingAction markerAction = new RecordingAction();

        final SignalChannelReader genericChannel = mock(SignalChannelReader.class);

        when(genericChannel.name()).thenReturn("generic");
        when(genericChannel.read()).thenAnswer(invocation -> List.of(new SignalRecord("marker", "marker", "{}", Map.of("channelOffset", -1L))));

        signalProcess = new SignalProcessor<>(SourceConnector.class, baseConfig(),
                Map.of("custom", action, "marker", markerAction),
                List.of(sourceChannel, genericChannel), documentReader, initialOffset);

        try {
            signalProcess.start();

            assertThat(sourceChannel.process(sourceSignalEvent("signal-1", "custom")))
                    .as("the signal must be accepted by the source signal channel")
                    .isTrue();

            // A cycle that completes after the signal was queued proves that the periodic cycle ran and still
            // did not execute the source channel signal.
            final int markersBefore = markerAction.invocations.get();
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(markerAction.invocations.get()).isGreaterThan(markersBefore));

            assertThat(action.invocations.get())
                    .as("the periodic cycle must not execute a source channel signal")
                    .isZero();
            assertThat(sourceChannel.signals)
                    .as("the source channel signal must still be queued")
                    .hasSize(1);

            // The signal is not lost, the streaming thread executes it when it processes the source signals.
            signalProcess.processSourceSignal(initialOffset.getTheOnlyPartition());

            assertThat(action.invocations.get()).isEqualTo(1);
            assertThat(action.executingThread.get())
                    .as("the source channel signal must be executed by the streaming thread")
                    .isSameAs(Thread.currentThread());
        }
        finally {
            signalProcess.stop();
        }
    }

    /**
     * Builds the change event that a connector delivers for a document of its signalling collection.
     */
    private static Struct sourceSignalEvent(String id, String type) {

        final Schema signalSchema = SchemaBuilder.struct().name("io.debezium.pipeline.signal.source")
                .field("id", Schema.STRING_SCHEMA)
                .field("type", Schema.STRING_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .build();

        final Schema envelopeSchema = SchemaBuilder.struct().name("io.debezium.pipeline.signal.envelope")
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.AFTER, signalSchema)
                .build();

        return new Struct(envelopeSchema)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code())
                .put(Envelope.FieldName.AFTER, new Struct(signalSchema)
                        .put("id", id)
                        .put("type", type)
                        .put("data", "{}"));
    }

    private static Semaphore signalProcessorSemaphore(SignalProcessor<?, ?> processor) throws Exception {
        final java.lang.reflect.Field field = SignalProcessor.class.getDeclaredField("semaphore");
        field.setAccessible(true);
        return (Semaphore) field.get(processor);
    }

    protected CommonConnectorConfig baseConfig() {
        return baseConfig(Map.of());
    }

    protected CommonConnectorConfig baseConfig(Map<String, Object> additionalConfig) {
        Configuration.Builder confBuilder = Configuration.create()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "core")
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 100)
                .with(CommonConnectorConfig.SIGNAL_EMIT_FAILURE_MAX_RETRIES, 9)
                .with(CommonConnectorConfig.SIGNAL_EMIT_FAILURE_BACKOFF_INTERVAL_MS, 9000)
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source,generic");

        additionalConfig.forEach(confBuilder::with);
        return new CommonConnectorConfig(confBuilder.build(), 0) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return null;
            }

            @Override
            public String getContextName() {
                return null;
            }

            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public EnumeratedValue getSnapshotMode() {
                return null;
            }

            @Override
            public Optional<EnumeratedValue> getSnapshotLockingMode() {
                return Optional.empty();
            }
        };
    }

    /**
     * A signal action that records how many times it was invoked and on which thread.
     */
    private static class RecordingAction implements SignalAction<TestPartition> {

        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicReference<Thread> executingThread = new AtomicReference<>();

        @Override
        public boolean arrived(SignalPayload<TestPartition> signalPayload) {
            invocations.incrementAndGet();
            executingThread.set(Thread.currentThread());
            return true;
        }
    }

    /**
     * A signal action that blocks the thread executing it, so that the processor permit is held for a while.
     */
    private static class BlockingAction implements SignalAction<TestPartition> {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public boolean arrived(SignalPayload<TestPartition> signalPayload) throws InterruptedException {
            started.countDown();
            release.await(30, TimeUnit.SECONDS);
            return true;
        }
    }

    /**
     * A signal channel whose read blocks until it is released, mimicking {@code KafkaSignalChannel#read()} waiting for
     * the poll timeout.
     */
    private static class BlockingSignalChannel implements SignalChannelReader {

        private final String name;
        private final CountDownLatch readStarted = new CountDownLatch(1);
        private final CountDownLatch readCompleted = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        BlockingSignalChannel(String name) {
            this.name = name;
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
            finally {
                readCompleted.countDown();
            }
            return List.of();
        }

        @Override
        public void close() {
            release.countDown();
        }
    }

    /**
     * A signal action that opts into synchronous invocation and delegates to the given action.
     */
    private static class SynchronousAction implements SignalAction<TestPartition> {

        private final SignalAction<TestPartition> delegate;

        SynchronousAction(SignalAction<TestPartition> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean arrived(SignalPayload<TestPartition> signalPayload) throws InterruptedException {
            return delegate.arrived(signalPayload);
        }

        @Override
        public boolean isSynchronous() {
            return true;
        }
    }

    private static class TestPartition implements Partition {
        @Override
        public Map<String, String> getSourcePartition() {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestOffset extends CommonOffsetContext {

        TestOffset(BaseSourceInfo sourceInfo) {
            super(sourceInfo);
        }

        @Override
        public Map<String, ?> getOffset() {
            return Map.of("LSN", 12345);
        }

        @Override
        public Schema getSourceInfoSchema() {
            return null;
        }

        @Override
        public boolean isInitialSnapshotRunning() {
            return false;
        }

        @Override
        public void preSnapshotStart(boolean onDemand) {

        }

        @Override
        public void preSnapshotCompletion() {

        }

        @Override
        public void event(DataCollectionId collectionId, Instant timestamp) {

        }

        @Override
        public TransactionContext getTransactionContext() {
            return null;
        }
    }

}
