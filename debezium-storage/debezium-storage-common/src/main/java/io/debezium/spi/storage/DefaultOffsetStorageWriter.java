/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.common.annotation.Incubating;

/**
 * Default implementation of {@link OffsetStorageWriter} that works with any {@link OffsetStore}.
 * <p>
 * Buffers offset writes in memory and flushes them to the backing store on demand.
 * Uses Jackson for JSON serialization, producing the same byte format as Kafka Connect's
 * {@code JsonConverter} with {@code schemas.enable=false}:
 * <ul>
 *   <li>Key format: {@code ["namespace", {"field": "value", ...}]} (JSON array)</li>
 *   <li>Value format: {@code {"field": value, ...}} (JSON object)</li>
 * </ul>
 * <p>
 * This class is thread-safe.
 *
 * @author Debezium Authors
 */
@Incubating
public class DefaultOffsetStorageWriter implements OffsetStorageWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOffsetStorageWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OffsetStore backingStore;
    private final String namespace;

    @SuppressWarnings("unchecked")
    private Map<Map<String, Object>, Map<String, Object>> data = new HashMap<>();
    private Map<Map<String, Object>, Map<String, Object>> toFlush = null;
    private final Semaphore flushInProgress = new Semaphore(1);
    private long currentFlushId = 0;

    public DefaultOffsetStorageWriter(OffsetStore backingStore, String namespace) {
        this.backingStore = backingStore;
        this.namespace = namespace;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void offset(Map<String, ?> partition, Map<String, ?> offset) {
        data.put((Map<String, Object>) partition, (Map<String, Object>) offset);
    }

    @Override
    public boolean beginFlush(long timeout, TimeUnit timeUnit) {
        try {
            if (flushInProgress.tryAcquire(Math.max(0, timeout), timeUnit)) {
                synchronized (this) {
                    if (data.isEmpty()) {
                        flushInProgress.release();
                        return false;
                    }
                    else {
                        toFlush = data;
                        data = new HashMap<>();
                        return true;
                    }
                }
            }
            else {
                throw new TimeoutException("Timed out waiting for previous flush to finish");
            }
        }
        catch (InterruptedException | TimeoutException e) {
            return false;
        }
    }

    @Override
    public synchronized void cancelFlush() {
        if (toFlush != null) {
            toFlush.putAll(data);
            data = toFlush;
            currentFlushId++;
            flushInProgress.release();
            toFlush = null;
        }
    }

    @Override
    public Future<Void> doFlush(OffsetStore.Callback<Void> callback) {
        final long flushId;
        final Map<ByteBuffer, ByteBuffer> offsetsSerialized;

        synchronized (this) {
            flushId = currentFlushId;

            try {
                offsetsSerialized = new HashMap<>(toFlush.size());
                for (Map.Entry<Map<String, Object>, Map<String, Object>> entry : toFlush.entrySet()) {
                    byte[] key = MAPPER.writeValueAsBytes(Arrays.asList(namespace, entry.getKey()));
                    ByteBuffer keyBuffer = ByteBuffer.wrap(key);
                    byte[] value = MAPPER.writeValueAsBytes(entry.getValue());
                    ByteBuffer valueBuffer = ByteBuffer.wrap(value);
                    offsetsSerialized.put(keyBuffer, valueBuffer);
                }
            }
            catch (JsonProcessingException e) {
                LOGGER.error("Failed to serialize offset data for namespace {}", namespace, e);
                if (callback != null) {
                    callback.onCompletion(e, null);
                }
                cancelFlush();
                return null;
            }
        }

        return backingStore.set(offsetsSerialized, (error, result) -> {
            boolean isCurrent = handleFinishWrite(flushId, error, result);
            if (isCurrent && callback != null) {
                callback.onCompletion(error, result);
            }
        });
    }

    private synchronized boolean handleFinishWrite(long flushId, Throwable error, Void result) {
        if (flushId != currentFlushId) {
            return false;
        }

        if (error != null) {
            cancelFlush();
        }
        else {
            currentFlushId++;
            flushInProgress.release();
            toFlush = null;
        }
        return true;
    }
}
