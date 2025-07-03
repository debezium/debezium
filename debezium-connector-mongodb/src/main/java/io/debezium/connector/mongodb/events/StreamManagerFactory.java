/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.events;

import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbOffsetContext;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.MultiTaskWindowHandler;
import io.debezium.connector.mongodb.ResumeTokens;

/**
 * Factory for creating an appropriate {@link StreamManager} based on the connector configuration
 */
public class StreamManagerFactory {
    public static <TResult> StreamManager<TResult> create(MongoDbOffsetContext offsetContext,
                                                          MongoDbConnectorConfig config,
                                                          MongoDbTaskContext taskContext) {
        if (config.isMultiTaskEnabled()) {
            return new MultiTaskStreamManager<>(offsetContext, config, taskContext);
        }
        return new DefaultStreamManager<>(offsetContext);
    }
}

/**
 * Default stream manager that does not manage the stream in any special way
 * @param <TResult> The document type of the stream managed by this manager
 */
class DefaultStreamManager<TResult> implements StreamManager<TResult> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStreamManager.class);
    private final MongoDbOffsetContext offsetContext;

    DefaultStreamManager(MongoDbOffsetContext offsetContext) {
        this.offsetContext = offsetContext;
    }

    @Override
    public ChangeStreamIterable<TResult> initStream(ChangeStreamIterable<TResult> stream) {
        if (offsetContext.lastResumeToken() != null) {
            LOGGER.info("Resuming streaming from token '{}'", offsetContext.lastResumeToken());
            stream.resumeAfter(offsetContext.lastResumeTokenDoc());
        }
        else if (offsetContext.lastTimestamp() != null) {
            LOGGER.info("Resuming streaming from operation time '{}'", offsetContext.lastTimestamp());
            stream.startAtOperationTime(offsetContext.lastTimestamp());
        }
        return stream;
    }

    @Override
    public ChangeStreamIterable<TResult> updateStream(ChangeStreamIterable<TResult> stream) {
        return stream;
    }

    @Override
    public boolean shouldUpdateStream(BufferingChangeStreamCursor.ResumableChangeStreamEvent<TResult> event) {
        return false;
    }
}

/**
 * Stream manager for window based multi-task mode
 * @param <TResult> The document type of the stream managed by this manager
 */
class MultiTaskStreamManager<TResult> implements StreamManager<TResult> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTaskStreamManager.class);

    private MultiTaskWindowHandler windowHandler;
    BsonTimestamp lastTimestamp;

    MultiTaskStreamManager(MongoDbOffsetContext offsetContext, MongoDbConnectorConfig config, MongoDbTaskContext taskContext) {
        if (!config.isMultiTaskEnabled()) {
            throw new IllegalArgumentException("Cannot create a multi-task stream manager without multi-task enabled");
        }
        MultiTaskWindowHandler windowHandler = new MultiTaskWindowHandler(
                config.getMultiTaskHopSeconds(),
                config.getMaxTasks(),
                taskContext.getMongoTaskId());
        BsonTimestamp startTime = offsetContext.lastResumeTokenTime();
        if (startTime == null) {
            startTime = offsetContext.lastTimestamp();
        }
        if (startTime != null) {
            windowHandler = windowHandler.startAtTimestamp(startTime);
            LOGGER.info("Setting window for stepwise taskId '{}'/'{}' start '{}' stop '{}'.",
                    windowHandler.taskId,
                    windowHandler.taskCount,
                    windowHandler.optimizedWindowStart,
                    windowHandler.windowStop);
        }
        this.windowHandler = windowHandler;
    }

    @Override
    public ChangeStreamIterable<TResult> initStream(ChangeStreamIterable<TResult> stream) {
        if (windowHandler.started) {
            stream.startAtOperationTime(windowHandler.optimizedWindowStart);
        }
        return stream;
    }

    @Override
    public ChangeStreamIterable<TResult> updateStream(ChangeStreamIterable<TResult> stream) {
        windowHandler = windowHandler.nextHop(lastTimestamp);
        LOGGER.info("task {} jump to next hop [{}-{}]",
                windowHandler.taskId,
                windowHandler.windowStart.getTime(),
                windowHandler.windowStop.getTime());
        return stream.startAtOperationTime(windowHandler.optimizedWindowStart);
    }

    @Override
    public boolean shouldUpdateStream(BufferingChangeStreamCursor.ResumableChangeStreamEvent<TResult> document) {
        if (!windowHandler.started) {
            windowHandler = windowHandler.startAtTimestamp(ResumeTokens.getTimestamp(document.resumeToken));
            LOGGER.info("Setting window for stepwise taskId '{}'/'{}' start '{}' stop '{}'.",
                    windowHandler.taskId,
                    windowHandler.taskCount,
                    windowHandler.optimizedWindowStart,
                    windowHandler.windowStop);
        }
        if (document.isEmpty()) {
            return false;
        }
        ChangeStreamDocument<TResult> event = document.document.get();
        BsonTimestamp timestamp = event.getClusterTime();
        if (timestamp.getTime() >= windowHandler.windowStop.getTime()) {
            LOGGER.debug("Event timestamp found {} compared to window stop timestamp {}", timestamp.getTime(), windowHandler.windowStop.getTime());
            lastTimestamp = timestamp;
            return true;
        }
        return false;
    }
}
