/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.batch.Batch;

/**
 * Interface for writing batches of records to the database.
 * Implementations may use different strategies (standard JDBC batching, UNNEST, etc.)
 *
 * @author Mario Fiore Vitale
 * @author Gaurav Miglani
 * @author rk3rn3r
 */
public interface RecordWriter {

    /**
     * Write a list of records to the database using the provided SQL statement information.
     *
     * @param tableDescriptor descriptor information of the table
     * @param records the list of records to write
     */
    void write(TableDescriptor tableDescriptor, List<JdbcSinkRecord> records);

    /**
     * Write a batch of records to the database.
     *
     * @param records the batch of records to write
     */
    void write(Batch records);

    /**
     * Checks if a record's schema and the table schema match or update table schema otherwise.
     *
     * @param collectionId The collection id of the table
     * @param record The record to check for matching schema and which schema will optionally update the table schema if needed
     * @return Returns the optionally updated table descriptor
     * @throws SQLException
     */
    TableDescriptor checkAndApplyTableChangesIfNeeded(CollectionId collectionId, JdbcSinkRecord record) throws SQLException;

    /**
     * Write a truncate statement to the database.
     *
     * @param collectionId The collection id of the table
     * @throws SQLException
     */
    void writeTruncate(CollectionId collectionId) throws SQLException;

    /**
     * Execute a callable with retries.
     *
     * @param description The description of the callable for log or error messages
     * @param callable The callable to execute
     * @return The result of the callable
     * @throws Exception
     */
    <T> T executeWithRetries(String description, Callable<T> callable);

    /**
     * Record containing SQL statement and metadata.
     *
     * @param statement The SQL statement
     * @param isBatchStatement Whether the statement is a batch statement
     */
    record SqlStatementInfo(String statement, boolean isBatchStatement, boolean isDelete) {
    }
}
