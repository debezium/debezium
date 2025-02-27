/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.connect.errors.RetriableException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.DelayStrategy;
import io.debezium.util.LoggingContext;

/**
 * Tests for {@link java.util.concurrent.Callable} with retries {@link RetryingCallable}.
 *
 * @author vjuranek
 */
public class RetryingCallableTest {

    private ExecutorService execService;

    @Before
    public void CreateExecutorService() {
        execService = Executors.newSingleThreadExecutor();
    }

    @After
    public void shutDownExecutorService() {
        execService.shutdownNow();
    }

    @Test
    public void shouldExecuteNeverFailing() throws InterruptedException, ExecutionException {
        final LogInterceptor interceptor = new LogInterceptor(RetryingCallable.class);
        Assertions.assertThat(execService.submit(new NeverFailing(0)).get()).isEqualTo(1);
        assertThat(interceptor.containsMessage("Failed with retriable exception")).isFalse();
    }

    @Test
    public void shouldNotRetryWhenCallableDoesNotFail() throws InterruptedException, ExecutionException {
        final LogInterceptor interceptor = new LogInterceptor(RetryingCallable.class);
        Assertions.assertThat(execService.submit(new NeverFailing(10)).get()).isEqualTo(1);
        assertThat(interceptor.containsMessage("Failed with retriable exception")).isFalse();
    }

    @Test
    public void shouldIgnoreInfiniteRetryWhenCallableDoesNotFail() throws InterruptedException, ExecutionException {
        final LogInterceptor interceptor = new LogInterceptor(RetryingCallable.class);
        Assertions.assertThat(execService.submit(new NeverFailing(EmbeddedEngineConfig.DEFAULT_ERROR_MAX_RETRIES)).get()).isEqualTo(1);
        assertThat(interceptor.containsMessage("Failed with retriable exception")).isFalse();
    }

    @Test
    public void shouldRetryAsManyTimesAsRequested() throws InterruptedException {
        final LogInterceptor interceptor = new LogInterceptor(RetryingCallable.class);
        LoggingContext.forConnector(getClass().getSimpleName(), "", "callable");

        TwoTimesFailing failing = new TwoTimesFailing(10);
        try {
            execService.submit(failing).get();
        }
        catch (ExecutionException e) {
            assertThat(e.getCause() instanceof RetriableException).isTrue();
        }

        // Callable should fail 2 times and 3rh time it should succeed.
        assertThat(failing.calls).isEqualTo(3);
        assertThat(interceptor.countOccurrences("Failed with retriable exception")).isEqualTo(2);
    }

    @Test
    public void shouldRetryAsManyTimesAsRequestedWhenAlwaysFails() throws InterruptedException {
        final LogInterceptor interceptor = new LogInterceptor(RetryingCallable.class);
        LoggingContext.forConnector(getClass().getSimpleName(), "", "callable");

        AlwaysFailing failing = new AlwaysFailing(5);
        try {
            execService.submit(failing).get();
        }
        catch (ExecutionException e) {
            assertThat(e.getCause() instanceof RetriableException).isTrue();
        }

        // Should be called 6 times - 1 call + 5 retries.
        assertThat(failing.calls).isEqualTo(6);
        // But we should see only 5 exception as the call was retried 5 times and on the 6th call failed, which is
        // not logged but thrown up to the stack.
        assertThat(interceptor.countOccurrences("Failed with retriable exception")).isEqualTo(5);
    }

    @Test
    public void shouldNotRetryWhenRetriesAreDisabled() throws InterruptedException {
        final LogInterceptor interceptor = new LogInterceptor(RetryingCallable.class);
        LoggingContext.forConnector(getClass().getSimpleName(), "", "callable");

        // 0 means that retries are disabled.
        AlwaysFailing failing = new AlwaysFailing(0);
        try {
            execService.submit(failing).get();
        }
        catch (ExecutionException e) {
            assertThat(e.getCause() instanceof RetriableException).isTrue();
        }

        // Should be called only 1 time.
        assertThat(failing.calls).isEqualTo(1);
        // And there shouldn't be any call in retry loop.
        assertThat(interceptor.containsMessage("Failed with retriable exception")).isFalse();
    }

    @Test
    public void shouldKeepRetryingWhenRetryIsInfinite() throws InterruptedException {
        final LogInterceptor interceptor = new LogInterceptor(RetryingCallable.class);
        LoggingContext.forConnector(getClass().getSimpleName(), "", "callable");

        // -1 means that retries are disabled.
        // Should fail if we change the config defaults, in such case loop in RetryingCallable needs to be adjusted!
        AlwaysFailing failing = new AlwaysFailing(EmbeddedEngineConfig.DEFAULT_ERROR_MAX_RETRIES);
        execService.submit(failing);
        Thread.sleep(3000);
        execService.shutdown();

        // Wait between the calls is 100 ms, so we should have at least 5 calls during 3 seconds sleep.
        assertThat(failing.calls).isGreaterThan(5);
        assertThat(interceptor.countOccurrences("Failed with retriable exception")).isGreaterThan(5);
    }

    private static class NeverFailing extends RetryingCallable<Integer> {

        protected volatile int calls;

        NeverFailing(final int retries) {
            super(retries);
            this.calls = 0;
        }

        public Integer doCall() throws Exception {
            calls++;
            return Integer.valueOf(calls);
        }

        @Override
        public DelayStrategy delayStrategy() {
            return DelayStrategy.linear(Duration.ofMillis(100));
        }
    }

    private static class AlwaysFailing extends NeverFailing {
        AlwaysFailing(final int retries) {
            super(retries);
        }

        public Integer doCall() throws Exception {
            super.doCall();
            throw new RetriableException("Good try, but I always fail");
        }
    }

    private static class TwoTimesFailing extends NeverFailing {
        TwoTimesFailing(final int retries) {
            super(retries);
        }

        public Integer doCall() throws Exception {
            super.doCall();
            if (calls <= 2) {
                throw new RetriableException(String.format("Good try, but I fail this time (call #%s)", calls));
            }
            return calls;
        }
    }
}
