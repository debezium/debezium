/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
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
import io.debezium.doc.FixFor;
import io.debezium.document.DocumentReader;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.signal.actions.Log;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.channels.SignalChannelReader;
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
