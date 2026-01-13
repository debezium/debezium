/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.List;

/**
 * Interface for writing batches of records to the database.
 * Implementations may use different strategies (standard JDBC batching, UNNEST, etc.)
 *
 * @author Mario Fiore Vitale
 * @author Gaurav Miglani
 */
public interface RecordWriter {

    /**
     * Write a list of records to the database using the provided SQL statement information.
     *
     * @param records the list of records to write
     * @param sqlStatementInfo the SQL statement and metadata about the statement
     */
    void write(List<JdbcSinkRecord> records, SqlStatementInfo sqlStatementInfo);

    /**
     * Record containing SQL statement and metadata.
     */
    record SqlStatementInfo(String statement, boolean isBatchStatement) {
    }
}
