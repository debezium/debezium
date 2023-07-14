/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Assert;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.util.LoggingContext;

public class ErrorHandlerTest {

    private static final class TestConnectorConfig extends CommonConnectorConfig {

        protected TestConnectorConfig(Configuration config) {
            super(config, 0);
        }

        @Override
        public String getContextName() {
            return "test";
        }

        @Override
        public String getConnectorName() {
            return "test";
        }

        @Override
        protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
            return null;
        }

    }

    @Test
    public void noError() throws Exception {
        final ChangeEventQueue<DataChangeEvent> queue = queue();

        poll(queue);
    }

    @Test
    public void nonRetriableByDefault() throws Exception {
        final Configuration config = Configuration.empty();

        final ChangeEventQueue<DataChangeEvent> queue = queue();

        final Exception error = new IllegalArgumentException("This is my error");
        initErrorHandler(config, queue, error);
        pollAndAssertNonRetriable(queue);
    }

    @Test
    public void isRetriable() throws Exception {
        final Configuration config = Configuration.create()
                .build();

        final ChangeEventQueue<DataChangeEvent> queue = queue();

        final Exception error = new IOException("This is my error");
        initErrorHandler(config, queue, error);
        pollAndAssertRetriable(queue);
    }

    @Test
    public void isRetryingWithMaxTimes() throws Exception {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.MAX_RETRIES_ON_ERROR, 2)
                .build();

        final ChangeEventQueue<DataChangeEvent> queue = queue();
        final var error = new IOException("This is my error");

        // First two attempt should result in retriable exception
        var errorHandler = initErrorHandler(config, queue, error);
        pollAndAssertRetriable(queue);

        // Second error is caused by retriable exception
        errorHandler = replaceErrorHandler(config, queue, new Exception(error), errorHandler);
        pollAndAssertRetriable(queue);

        // Last attempt should result in non-retriable exception
        replaceErrorHandler(config, queue, error, errorHandler);
        pollAndAssertNonRetriable(queue);
    }

    @Test
    public void isNotRetryingWithMaxRetries() throws Exception {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.MAX_RETRIES_ON_ERROR, 2)
                .build();

        final ChangeEventQueue<DataChangeEvent> queue = queue();
        final var error = new IOException("This is my error");

        // First attempt should result in retriable exception
        var errorHandler = initErrorHandler(config, queue, error);
        pollAndAssertRetriable(queue);

        // Second attempt should result in non-retriable exception
        final var fatalError = new Exception("This is fatal");
        replaceErrorHandler(config, queue, fatalError, errorHandler);
        pollAndAssertNonRetriable(queue);
    }

    @Test
    public void customRetriableMatch() throws Exception {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.CUSTOM_RETRIABLE_EXCEPTION, ".*my error.*")
                .build();
        final ChangeEventQueue<DataChangeEvent> queue = queue();

        final Exception error = new IllegalArgumentException("This is my error to retry");
        initErrorHandler(config, queue, error);
        pollAndAssertRetriable(queue);
    }

    @Test
    public void customRetriableNoMatch() throws Exception {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.CUSTOM_RETRIABLE_EXCEPTION, ".*not my error.*")
                .build();
        final ChangeEventQueue<DataChangeEvent> queue = queue();

        final Exception error = new IllegalArgumentException("This is my error to retry");
        initErrorHandler(config, queue, error);
        pollAndAssertNonRetriable(queue);
    }

    @Test
    public void customRetriableMatchNested() throws Exception {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.CUSTOM_RETRIABLE_EXCEPTION, ".*my error.*")
                .build();
        final ChangeEventQueue<DataChangeEvent> queue = queue();

        final Exception error = new IllegalArgumentException("This is my error to retry");
        initErrorHandler(config, queue, new Exception("Main", error));
        pollAndAssertRetriable(queue);
    }

    private void pollAndAssertRetriable(ChangeEventQueue<DataChangeEvent> queue) throws Exception {
        try {
            poll(queue);
            Assert.fail("Exception must be thrown");
        }
        catch (ConnectException e) {
            assertThat(e).isInstanceOf(RetriableException.class);
        }
    }

    private void pollAndAssertNonRetriable(ChangeEventQueue<DataChangeEvent> queue) throws Exception {
        try {
            poll(queue);
            Assert.fail("Exception must be thrown");
        }
        catch (ConnectException e) {
            assertThat(e).isNotInstanceOf(RetriableException.class);
        }
    }

    private void poll(final ChangeEventQueue<DataChangeEvent> queue) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            queue.poll();
            Thread.sleep(100);
        }
    }

    private ErrorHandler initErrorHandler(final Configuration config,
                                          final ChangeEventQueue<DataChangeEvent> queue,
                                          Throwable producerThrowable) {
        return replaceErrorHandler(config, queue, producerThrowable, null);
    }

    private ErrorHandler replaceErrorHandler(final Configuration config,
                                             final ChangeEventQueue<DataChangeEvent> queue,
                                             Throwable producerThrowable,
                                             ErrorHandler replacedErrorHandler) {
        final ErrorHandler errorHandler = new ErrorHandler(SourceConnector.class, new TestConnectorConfig(config), queue, replacedErrorHandler);
        if (producerThrowable != null) {
            errorHandler.setProducerThrowable(producerThrowable);
        }
        return errorHandler;
    }

    private ChangeEventQueue<DataChangeEvent> queue() {
        final ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.ofMillis(1))
                .maxBatchSize(1000)
                .maxQueueSize(1000)
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "test", "test"))
                .build();
        return queue;
    }
}
