/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

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

    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value, Envelope.Operation operation) {

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
            default:
                break;
        }

        LOGGER.trace("Counter status create:{}, delete:{}, update:{}", createCount, deleteCount, updateCount);
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

    public void reset() {
        createCount.reset();
        updateCount.reset();
        deleteCount.reset();
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
