/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics.activity;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

public class ActivityMonitoringMeter implements ActivityMonitoringMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivityMonitoringMeter.class);

    private final ActivityCounter createCount = new ActivityCounter();
    private final ActivityCounter updateCount = new ActivityCounter();
    private final ActivityCounter deleteCount = new ActivityCounter();
    private final ActivityCounter truncateCount = new ActivityCounter();

    private boolean isPaused = false;

    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value, Envelope.Operation operation) {

        if (isPaused) {
            LOGGER.trace("ActivityMonitoringMeter is paused, no metric will be collected.");
            return;
        }

        LOGGER.trace("Received record {} with key {}", value, key);
        String tableName = source.identifier();
        switch (operation) {
            case CREATE:
                createCount.add(1, tableName);
                break;
            case UPDATE:
                updateCount.add(1, tableName);
                break;
            case DELETE:
                deleteCount.add(1, tableName);
                break;
            case TRUNCATE:
                truncateCount.add(1, tableName);
                break;
            default:
                break;
        }

        LOGGER.trace("Counter status create:{}, delete:{}, update:{}, truncate:{}", createCount, deleteCount, updateCount, truncateCount);
    }

    @Override
    public Map<String, Long> getNumberOfCreateEventsSeen() {
        return createCount.getCounter();
    }

    @Override
    public Map<String, Long> getNumberOfDeleteEventsSeen() {
        return deleteCount.getCounter();
    }

    @Override
    public Map<String, Long> getNumberOfUpdateEventsSeen() {
        return updateCount.getCounter();
    }

    @Override
    public Map<String, Long> getNumberOfTruncateEventsSeen() {
        return truncateCount.getCounter();
    }

    @Override
    public void pause() {
        isPaused = true;
    }

    @Override
    public void resume() {
        isPaused = false;
    }

    public void reset() {
        createCount.reset();
        updateCount.reset();
        deleteCount.reset();
        truncateCount.reset();
    }

    public static class ActivityCounter {

        private final ConcurrentMap<String, AtomicLong> counterByCollection = new ConcurrentHashMap<>();

        public ActivityCounter() {
        }

        public void add(int increment, String tableName) {

            counterByCollection.compute(tableName, (k, v) -> {

                if (v == null) {
                    return new AtomicLong(increment);
                }

                v.addAndGet(increment);

                return v;
            });

        }

        public Map<String, Long> getCounter() {
            return counterByCollection.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
        }

        public void reset() {
            counterByCollection.clear();
        }
    }
}
