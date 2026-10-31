/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.relational.TableId;

/**
 * Resolves the definition of a table created with {@code CREATE TABLE ... AS SELECT}.
 * <p>
 * The statement carries no column list, so the connector reads the table's {@code CREATE TABLE}
 * statement from the database as it stood when the statement's transaction committed, and parses
 * that in its place.
 *
 * @author Chris Cranford
 */
@FunctionalInterface
public interface CreateTableAsSelectResolver {

    /**
     * Resolves the {@code CREATE TABLE} statement of the given table.
     *
     * @param tableId the table the statement created, never {@code null}
     * @return the statement to parse in place of the {@code CREATE TABLE AS SELECT}, or {@code null}
     *         when it cannot be resolved and the schema change is to be skipped
     * @throws InterruptedException if the thread is interrupted
     */
    String resolveCreateTableDdl(TableId tableId) throws InterruptedException;
}
