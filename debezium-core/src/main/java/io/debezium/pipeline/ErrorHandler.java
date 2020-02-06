/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;

public class ErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandler.class);

    private static final Duration MAX_BACK_OFF = Duration.ofMinutes(1);

    private final ChangeEventSourceFacade facade;
    private final ChangeEventQueue<?> queue;
    private final RetryableErrorRecognitionStrategy retryableErrorRecognitionStrategy;
    private final AtomicReference<Throwable> producerThrowable;
    private final ExecutorService executor;

    private Duration currentBackOff = Duration.ofSeconds(3);
    private Instant lastErrorOccuranceInstant;

    public ErrorHandler(Class<? extends SourceConnector> connectorType, String logicalName,
                        ChangeEventSourceFacade facade,
                        RetryableErrorRecognitionStrategy retryableErrorRecognitionStrategy) {
        this.facade = facade;
        this.queue = facade.getQueue();
        this.retryableErrorRecognitionStrategy = retryableErrorRecognitionStrategy;
        this.executor = Threads.newSingleThreadExecutor(connectorType, logicalName, "error-handler");
        this.producerThrowable = new AtomicReference<>();
    }

    public void handleThrowable(Throwable producerThrowable) {
        LOGGER.error("Producer failure", producerThrowable);

        if (retryableErrorRecognitionStrategy.isRetryable(producerThrowable)) {

            multiplyOrResetBackOff();

            facade.stop();

            try {
                LOGGER.info("Backing off for {}", currentBackOff);
                Metronome.parker(currentBackOff, Clock.SYSTEM).pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            facade.start();
        }
        else {

            boolean first = this.producerThrowable.compareAndSet(null, producerThrowable);

            if (first) {
                queue.producerFailure(producerThrowable);
                executor.execute(facade::stop);
            }
        }
    }

    private void multiplyOrResetBackOff() {
        if (lastOccuranceWasMoreThan10MinutesAgo()) {
            resetCurrentBackOff();
        }
        else {
            multiplyCurrentBackOff();
        }
        lastErrorOccuranceInstant = Instant.now();
    }

    private boolean lastOccuranceWasMoreThan10MinutesAgo() {
        return lastErrorOccuranceInstant != null &&
                Instant.now().minus(Duration.ofMinutes(10)).isAfter(lastErrorOccuranceInstant);
    }

    private void resetCurrentBackOff() {
        currentBackOff = Duration.ofSeconds(3);
    }

    private void multiplyCurrentBackOff() {
        currentBackOff = currentBackOff.multipliedBy(2);
        if (currentBackOff.toMillis() > MAX_BACK_OFF.toMillis()) {
            currentBackOff = MAX_BACK_OFF;
        }
    }

    public void stop() throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(60, TimeUnit.SECONDS);
    }
}
