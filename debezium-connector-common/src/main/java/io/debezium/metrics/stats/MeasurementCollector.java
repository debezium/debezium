/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.stats;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.metrics.event.MeasurementEvent;

/**
 * // TODO: Document this
 * @author vjuranek
 * @since 4.0
 */
public class MeasurementCollector<T extends MeasurementEvent> implements Consumer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MeasurementCollector.class);
    private static final int QUEUE_SIZE = 1000;

    private final Map<Class<T>, Measurement<T>> measurements = new ConcurrentHashMap<>();
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>(QUEUE_SIZE);
    private final Thread publisherThread = new Thread(new MeasurementPublisher());

    public MeasurementCollector() {
        publisherThread.setUncaughtExceptionHandler((t, ex) -> LOGGER.warn("Publisher thread failed with exception: ", ex));
        publisherThread.start();
    }

    @Override
    public void accept(T measurementEvent) {
        Measurement<T> measurement = measurements.get(measurementEvent.getClass());
        if (measurement != null) {
            try {
                if (!queue.offer(measurementEvent)) {
                    LOGGER.warn("Failed to add measurement event to queue, queue is full");
                }
            }
            catch (Exception e) {
                LOGGER.warn("Failed to add measurement value for measurement event {}", measurementEvent, e);
            }
        }
    }

    public void addMeasurement(Class<T> clazz, Measurement<T> measurement) {
        if (measurements.containsKey(clazz)) {
            throw new DebeziumException("Duplicate measurement event for measurement class " + clazz.getName());
        }
        measurements.put(clazz, measurement);
    }

    public void start() {
        publisherThread.start();
    }

    public void stop() {
        queue.clear();
        publisherThread.interrupt();
    }

    // TODO explain why we need publishing via queue
    private class MeasurementPublisher implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                T event;
                try {
                    event = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (event == null) {
                        continue;
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                Measurement<T> measurement = measurements.get(event.getClass());
                measurement.accept(event);
            }
        }
    }
}
