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

    TableDescriptor checkAndApplyTableChangesIfNeeded(CollectionId collectionId, JdbcSinkRecord record) throws SQLException;

    void writeTruncate(CollectionId collectionId) throws SQLException;

    <T> T executeWithRetries(String description, Callable<T> callable);

    /**
     * Record containing SQL statement and metadata.
     */
    record SqlStatementInfo(String statement, boolean isBatchStatement) {
    }
}
