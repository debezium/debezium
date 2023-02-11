/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

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

        final ErrorHandler errorHandler = errorHandler(config, queue);
        final Exception error = new IllegalArgumentException("This is my error");
        errorHandler.setProducerThrowable(error);
        try {
            poll(queue);
            Assert.fail("Exception must be thrown");
        }
        catch (ConnectException e) {
            assertThat(e instanceof RetriableException).isFalse();
        }
    }

    private void poll(final ChangeEventQueue<DataChangeEvent> queue) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            queue.poll();
            Thread.sleep(100);
        }
    }

    @Test
    public void customRetriableMatch() throws Exception {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.CUSTOM_RETRIABLE_EXCEPTION, ".*my error.*")
                .build();
        final ChangeEventQueue<DataChangeEvent> queue = queue();

        final ErrorHandler errorHandler = errorHandler(config, queue);
        final Exception error = new IllegalArgumentException("This is my error to retry");
        errorHandler.setProducerThrowable(error);
        try {
            poll(queue);
            Assert.fail("Exception must be thrown");
        }
        catch (ConnectException e) {
            assertThat(e instanceof RetriableException).isTrue();
        }
    }

    @Test
    public void customRetriableNoMatch() throws Exception {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.CUSTOM_RETRIABLE_EXCEPTION, ".*not my error.*")
                .build();
        final ChangeEventQueue<DataChangeEvent> queue = queue();

        final ErrorHandler errorHandler = errorHandler(config, queue);
        final Exception error = new IllegalArgumentException("This is my error to retry");
        errorHandler.setProducerThrowable(error);
        try {
            poll(queue);
            Assert.fail("Exception must be thrown");
        }
        catch (ConnectException e) {
            assertThat(e instanceof RetriableException).isFalse();
        }
    }

    @Test
    public void customRetriableMatchNested() throws Exception {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.CUSTOM_RETRIABLE_EXCEPTION, ".*my error.*")
                .build();
        final ChangeEventQueue<DataChangeEvent> queue = queue();

        final ErrorHandler errorHandler = errorHandler(config, queue);
        final Exception error = new IllegalArgumentException("This is my error to retry");
        errorHandler.setProducerThrowable(new Exception("Main", error));
        try {
            poll(queue);
            Assert.fail("Exception must be thrown");
        }
        catch (ConnectException e) {
            assertThat(e instanceof RetriableException).isTrue();
        }
    }

    private ErrorHandler errorHandler(final Configuration config, final ChangeEventQueue<DataChangeEvent> queue) {
        final ErrorHandler errorHandler = new ErrorHandler(SourceConnector.class, new TestConnectorConfig(config),
                queue);
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
