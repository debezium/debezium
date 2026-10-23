/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.doc.FixFor;
import io.debezium.embedded.TestingDebeziumEngine;
import io.debezium.engine.DebeziumEngine;

class ConnectorTestShutdownTest extends AbstractAsyncEngineConnectorTest {
    private boolean requestStopDuringClose;

    @Override
    protected TestingDebeziumEngine<SourceRecord> createEngine(DebeziumEngine.Builder<SourceRecord> builder) {
        return new TestingAsyncEmbeddedEngine<>((AsyncEmbeddedEngine<SourceRecord>) builder.build()) {
            @Override
            public void close() throws IOException {
                if (requestStopDuringClose) {
                    BlockingTask.allowRecord.countDown();
                    try {
                        assertThat(BlockingTask.stopping.await(10, TimeUnit.SECONDS)).isTrue();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                }
                super.close();
            }
        };
    }

    @Test
    void shouldWaitWhenConsumerRequestsShutdownDuringClose() throws Exception {
        BlockingTask.reset();
        requestStopDuringClose = true;
        start(BlockingConnector.class, Configuration.create().build(), record -> true);
        assertThat(BlockingTask.polling.await(10, TimeUnit.SECONDS)).isTrue();
        assertCleanupWaitsForTask();
    }

    @Test
    void shouldWaitForConsumerRequestedShutdownAndAllowRestart() throws Exception {
        BlockingTask.reset();
        start(BlockingConnector.class, Configuration.create().build(), record -> true);
        BlockingTask.allowRecord.countDown();
        try {
            assertThat(BlockingTask.stopping.await(10, TimeUnit.SECONDS)).isTrue();
            assertCleanupWaitsForTask();
        }
        finally {
            BlockingTask.allowStop.countDown();
        }

        // The next engine must still be closed normally, rather than waiting for the previous stop request.
        BlockingTask.reset();
        start(BlockingConnector.class, Configuration.create().build());
        assertThat(BlockingTask.polling.await(10, TimeUnit.SECONDS)).isTrue();
        final var cleanup = CompletableFuture.runAsync(this::stopConnector);
        try {
            assertThat(BlockingTask.stopping.await(10, TimeUnit.SECONDS)).isTrue();
        }
        finally {
            BlockingTask.allowStop.countDown();
            cleanup.get(15, TimeUnit.SECONDS);
        }
        assertConnectorNotRunning();
    }

    @Test
    void shouldWaitForTaskDuringNormalShutdown() throws Exception {
        BlockingTask.reset();
        start(BlockingConnector.class, Configuration.create().build());
        assertThat(BlockingTask.polling.await(10, TimeUnit.SECONDS)).isTrue();
        assertCleanupWaitsForTask();
    }

    @Test
    @FixFor("debezium/dbz#2932")
    void shouldStopRightAfterStartWhenTasksStartAtDifferentTimes() throws Exception {
        SlowStartTask.reset();
        start(SlowStartConnector.class, Configuration.create().with(CommonConnectorConfig.TASKS_MAX, 2).build());
        // One task reports it has started while the other is still starting; start() must not return in that window.
        stopConnector();
        assertConnectorNotRunning();
        assertThat(engine).isNull();
    }

    private void assertCleanupWaitsForTask() throws Exception {
        final var stillRunning = new AtomicReference<Boolean>();
        final var cleanup = CompletableFuture.runAsync(() -> stopConnector(stillRunning::set));
        try {
            assertThat(BlockingTask.stopping.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> cleanup.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(stillRunning.get()).isNull();
        }
        finally {
            BlockingTask.allowStop.countDown();
            cleanup.get(15, TimeUnit.SECONDS);
        }
        assertThat(stillRunning.get()).isFalse();
        assertConnectorNotRunning();
        assertThat(engine).isNull();
    }

    public static class BlockingConnector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return BlockingTask.class;
        }
    }

    public static class SlowStartConnector extends DebeziumAsyncEngineTestUtils.MultiTaskSimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return SlowStartTask.class;
        }
    }

    public static class SlowStartTask extends SimpleSourceConnector.SimpleConnectorTask {
        private static final AtomicInteger startedTasks = new AtomicInteger();

        private static void reset() {
            startedTasks.set(0);
        }

        @Override
        public void start(Map<String, String> props) {
            // Every task except the first one is slow to start, keeping the engine in STARTING_TASKS for a while.
            if (startedTasks.getAndIncrement() > 0) {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            super.start(props);
        }
    }

    public static class BlockingTask extends SimpleSourceConnector.SimpleConnectorTask {
        private static CountDownLatch polling;
        private static CountDownLatch allowRecord;
        private static CountDownLatch stopping;
        private static CountDownLatch allowStop;

        private static void reset() {
            polling = new CountDownLatch(1);
            allowRecord = new CountDownLatch(1);
            stopping = new CountDownLatch(1);
            allowStop = new CountDownLatch(1);
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            polling.countDown();
            allowRecord.await();
            return super.poll();
        }

        @Override
        public void stop() {
            stopping.countDown();
            try {
                if (!allowStop.await(15, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Test did not release task shutdown");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            finally {
                super.stop();
            }
        }
    }
}
