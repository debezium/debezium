/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.List;

/**
 * An interface for implementing several kind of buffers
 *
 * @author Gaurav Miglani
 */
public interface Buffer {

    /**
     * to add a {@link SinkRecordDescriptor} to the internal buffer and
     * call the {@link Buffer#flush()} when buffer size >= {@link JdbcSinkConnectorConfig#getBatchSize()}
     * @param recordDescriptor the Sink record descriptor
     * @return the  buffer  records
     */
    List<SinkRecordDescriptor> add(SinkRecordDescriptor recordDescriptor);

    /**
     * to clear and flush the internal buffer
     * @return {@link SinkRecordDescriptor} the flushed buffer records.
     */
    List<SinkRecordDescriptor> flush();

    /**
     * to check whether buffer is empty or not.
     * @return true if empty else false.
     */
    boolean isEmpty();
}
