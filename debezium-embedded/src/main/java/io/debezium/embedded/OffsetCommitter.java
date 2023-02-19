/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * Interface for committing offsets.
 */
public interface OffsetCommitter {

    void maybeFlush() throws InterruptedException;

    void commitOffsets() throws InterruptedException;

    void commit(SourceRecord record) throws InterruptedException;
}
