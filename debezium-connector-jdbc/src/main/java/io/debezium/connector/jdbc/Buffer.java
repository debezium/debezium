/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.List;

import io.debezium.connector.jdbc.relational.TableDescriptor;

/**
 * An interface for implementing several kind of buffers
 *
 * @author Gaurav Miglani
 */
public interface Buffer {

    /**
     * to add a {@link JdbcSinkRecord} to the internal buffer and
     * call the {@link Buffer#flush()} when buffer size >= {@link JdbcSinkConnectorConfig#getBatchSize()}
     * @param record the Debezium sink record
     * @return the  buffer  records
     */
    List<JdbcSinkRecord> add(JdbcSinkRecord record);

    /**
     * to clear and flush the internal buffer
     * @return {@link JdbcSinkRecord} the flushed buffer records.
     */
    List<JdbcSinkRecord> flush();

    /**
     * to check whether buffer is empty or not.
     * @return true if empty else false.
     */
    boolean isEmpty();

    /**
     * to get the table descriptor
     * @return the table descriptor
     */
    TableDescriptor getTableDescriptor();
}
