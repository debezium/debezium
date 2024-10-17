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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceConnector;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.common.BaseSourceInfo;
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

public class SignalProcessorTest {

    private SignalProcessor<TestPartition, OffsetContext> signalProcess;
    private final DocumentReader documentReader = DocumentReader.defaultReader();

    private Offsets<TestPartition, OffsetContext> initialOffset;

    @Before
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

    protected CommonConnectorConfig baseConfig() {
        return baseConfig(Map.of());
    }

    protected CommonConnectorConfig baseConfig(Map<String, Object> additionalConfig) {
        Configuration.Builder confBuilder = Configuration.create()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "core")
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 100)
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
        public boolean isSnapshotRunning() {
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
