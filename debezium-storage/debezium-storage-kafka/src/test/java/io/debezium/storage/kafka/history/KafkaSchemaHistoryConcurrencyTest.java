/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.history;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.relational.history.SchemaHistoryListener;

class KafkaSchemaHistoryConcurrencyTest {

    private final List<Thread> callers = new ArrayList<>();
    private PausingExecutor executor;
    private KafkaSchemaHistory history;

    @AfterEach
    void cleanup() throws InterruptedException {
        if (executor != null) {
            executor.resumeSubmission.countDown();
            executor.resumeFirstCheck.countDown();
        }
        for (Thread caller : callers) {
            caller.join(TimeUnit.SECONDS.toMillis(10));
        }
        if (executor != null) {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor did not terminate");
        }
        for (Thread caller : callers) {
            assertFalse(caller.isAlive(), "Caller did not terminate: " + caller.getName());
        }
    }

    @RepeatedTest(10)
    @FixFor("debezium/dbz#2702")
    void shouldAcceptSubmissionRacingWithCompletedCheck() throws Exception {
        configureHistory(2, true);
        history.checkStorageSettings();
        awaitLatch(executor.firstCheckStarted);

        final var submission = startCaller("history-submission", history::checkStorageSettings);
        awaitLatch(executor.submissionPaused);

        // Finish the first check after the second caller has checked isShutdown(), but before execute() accepts its task.
        executor.resumeFirstCheck.countDown();
        await().atMost(10, TimeUnit.SECONDS).until(() -> executor.isShutdown() || isBlockedInHistoryShutdown(executor.worker));
        executor.resumeSubmission.countDown();

        submission.result().get(10, TimeUnit.SECONDS);
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(2, executor.getCompletedTaskCount());
    }

    @RepeatedTest(10)
    @FixFor("debezium/dbz#2702")
    void shouldAcceptSubmissionRacingWithStop() throws Exception {
        configureHistory(1, false);
        final var submission = startCaller("history-submission", history::checkStorageSettings);
        awaitLatch(executor.submissionPaused);

        final var stopping = startCaller("history-stop", history::stop);
        await().atMost(10, TimeUnit.SECONDS).until(() -> stopping.result().isDone() || isBlockedInHistoryShutdown(stopping.thread()));
        executor.resumeSubmission.countDown();

        submission.result().get(10, TimeUnit.SECONDS);
        stopping.result().get(10, TimeUnit.SECONDS);
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(1, executor.getCompletedTaskCount());
    }

    @Test
    @FixFor("debezium/dbz#2702")
    void shouldSkipSubmissionAfterStop() throws Exception {
        configureHistory(0, false);
        history.stop();

        assertDoesNotThrow(history::checkStorageSettings);
        assertEquals(0, executor.submissions.get());
        assertTrue(executor.isShutdown());
    }

    @Test
    @FixFor("debezium/dbz#2702")
    void shouldSubmitCheckWithoutWaitingForCompletion() throws Exception {
        configureHistory(0, true);
        final var submission = startCaller("history-submission", history::checkStorageSettings);
        awaitLatch(executor.firstCheckStarted);

        submission.result().get(10, TimeUnit.SECONDS);
        assertEquals(0, executor.getCompletedTaskCount());
        assertFalse(executor.isShutdown());

        executor.resumeFirstCheck.countDown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(1, executor.getCompletedTaskCount());
    }

    @Test
    @FixFor("debezium/dbz#2702")
    void shouldSkipCheckWithoutExecutor() {
        assertDoesNotThrow(new KafkaSchemaHistory()::checkStorageSettings);
    }

    private void configureHistory(int pausedSubmission, boolean pauseFirstCheck) throws ReflectiveOperationException {
        history = new KafkaSchemaHistory();
        final var config = Configuration.create()
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history-concurrency-test")
                // Fail AdminClient construction locally, then execute the real check-completion shutdown path.
                .with("schema.history.internal.producer." + AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "invalid")
                .build();
        history.configure(config, null, SchemaHistoryListener.NOOP, true);

        executor = new PausingExecutor(pausedSubmission, pauseFirstCheck);
        final var field = KafkaSchemaHistory.class.getDeclaredField("checkTopicSettingsExecutor");
        field.setAccessible(true);
        field.set(history, executor);
    }

    private Call startCaller(String name, Runnable action) {
        final var result = new FutureTask<Void>(action, null);
        final var thread = new Thread(result, name);
        thread.setDaemon(true);
        callers.add(thread);
        thread.start();
        return new Call(thread, result);
    }

    private static boolean isBlockedInHistoryShutdown(Thread thread) {
        return thread != null && thread.getState() == Thread.State.BLOCKED
                && Arrays.stream(thread.getStackTrace()).anyMatch(frame -> frame.getClassName().equals(KafkaSchemaHistory.class.getName())
                        && frame.getMethodName().equals("stopCheckTopicSettingsExecutor"));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for the controlled test interleaving");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private record Call(Thread thread, FutureTask<Void> result) {
    }

    private static class PausingExecutor extends ThreadPoolExecutor {

        private final int pausedSubmission;
        private final AtomicInteger submissions = new AtomicInteger();
        private final CountDownLatch submissionPaused = new CountDownLatch(1);
        private final CountDownLatch resumeSubmission = new CountDownLatch(1);
        private final CountDownLatch firstCheckStarted = new CountDownLatch(1);
        private final CountDownLatch resumeFirstCheck;
        private volatile Thread worker;

        private PausingExecutor(int pausedSubmission, boolean pauseFirstCheck) {
            super(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), task -> {
                final var thread = new Thread(task, "history-config-check");
                thread.setDaemon(true);
                return thread;
            });
            this.pausedSubmission = pausedSubmission;
            this.resumeFirstCheck = new CountDownLatch(pauseFirstCheck ? 1 : 0);
        }

        @Override
        public void execute(Runnable command) {
            if (submissions.incrementAndGet() == pausedSubmission) {
                submissionPaused.countDown();
                awaitLatch(resumeSubmission);
            }
            super.execute(command);
        }

        @Override
        protected void beforeExecute(Thread thread, Runnable command) {
            super.beforeExecute(thread, command);
            worker = thread;
            firstCheckStarted.countDown();
            awaitLatch(resumeFirstCheck);
        }
    }
}
