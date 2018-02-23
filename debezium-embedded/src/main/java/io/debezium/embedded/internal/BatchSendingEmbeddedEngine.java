/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.internal;

import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;

/**
 * A default implementation of EmbeddedEngine
 *
 * @author Randall Hauch, Jiri Pechanec
 *
 */
@ThreadSafe
public class BatchSendingEmbeddedEngine extends AbstractEmbeddedEngine {

    public BatchSendingEmbeddedEngine(Configuration config, ClassLoader classLoader, Clock clock, Consumer<SourceRecord> consumer,
                           CompletionCallback completionCallback, ConnectorCallback connectorCallback,
                           OffsetCommitPolicy offsetCommitPolicy) {
        super(config, classLoader, clock, consumer, completionCallback, connectorCallback, offsetCommitPolicy);
    }
}
